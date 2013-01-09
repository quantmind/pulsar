import os
import sys
import heapq
import logging
import traceback
import time
import signal
import errno
import socket
from functools import partial
from threading import current_thread

from pulsar import HaltServer, Timeout
from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform, Waker
from pulsar.utils.security import gen_unique_id
from pulsar.utils.log import Synchronized
from pulsar.utils.structures import WeakList

from .defer import Deferred, is_async, maybe_async, thread_loop, make_async,\
                    log_failure, EXIT_EXCEPTIONS

__all__ = ['IOLoop', 'PeriodicCallback', 'loop_timeout']

LOGGER = logging.getLogger('pulsar.eventloop')

if sys.version_info >= (3, 3):
    timer = time.monotonic
else:   #pragma    nocover
    timer = time.time
    
def file_descriptor(fd):
    if hasattr(fd, 'fileno'):
        return fd.fileno()
    else:
        return fd

def setid(self):
    self.tid = current_thread().ident
    self.pid = os.getpid()

class StopEventLoop(BaseException):
    """Raised to stop the event loop."""

def _raise_stop_event_loop():
    raise StopEventLoop


class TimedCall(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    def __init__(self, deadline, callback, args, canceller=None):
        self.deadline = deadline
        self.canceller = canceller
        self._callback = callback
        self._args = args
        self._cancelled = False

    def __lt__(self, other):
        return self.deadline < other.deadline
        
    @property
    def cancelled(self):
        return self._cancelled
    
    @property
    def callback(self):
        return self._callback
    
    @property
    def args(self):
        return self._args
    
    def cancel(self):
        '''Attempt to cancel the callback.'''
        if not self.cancelled:
            if self.canceller:
                self.canceller(self)
            self._cancelled = True

    def __call__(self):
        self._callback(*self._args)
        
        
class FileDescriptor(IObase):
    def __init__(self, fd, eventloop, read=None, write=None, connect=None):
        self.fd = fd
        self.eventloop = eventloop
        self._connecting = False
        self.handle_write = None
        self.handle_read = None
        if connect:
            self.add_connector(connect)
        elif write:
            self.add_writer(write)
        if read:
            self.add_reader(read)

    @property
    def poller(self):
        return self.eventloop._impl
    
    @property
    def reading(self):
        return bool(self.handle_read)
    
    @property
    def writing(self):
        return bool(self.handle_read)
    
    @property
    def connecting(self):
        return self._connecting
    
    @property
    def state(self):
        if self.reading:
            if self.writing:
                return self.READ | self.WRITE
            else:
                return self.READ
        elif self.writing:
            return self.WRITE
            
    @property
    def state_code(self):
        s = []
        if self.state is None:
            return 'closed'
        if self.connecting:
            s.append('connecting')
        elif self.writing:
            s.append('writing')
        if self.reading:
            s.append('reading')
        return ' '.join(s) if s else 'idle'
    
    def add_connector(self, callback):
        if self.state is not None:
            raise RuntimeError('Cannot connect. State is %s.' % self.state_code)
        self._connecting = True
        self.add_writer(callback)
        
    def add_reader(self, callback):
        if not self.handle_read:
            current_state = self.state
            self.handle_read = callback
            self.modify_state(current_state, self.READ)
        else:
            raise RuntimeError("Asynchronous stream already reading!")
        
    def add_writer(self, callback):
        if not self.handle_write:
            current_state = self.state
            self.handle_write = callback
            self.modify_state(current_state, self.WRITE)
        else:
            raise RuntimeError("Asynchronous stream already writing!")
        
    def remove_connector(self):
        self._connecting = False
        return self.remove_writer()
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        state = self.ERROR
        if self.writing:
            state |= self.WRITE
        self.handle_read = None
        self.modify_state(state)
        return self.writing

    def remove_writer(self):
        '''Remove writer and return True if reading'''
        state = self.ERROR
        if self.reading:
            state |= self.READ
        self.handle_write = None
        self.modify_state(state)
        return self.reading
    
    def __call__(self, events):
        if events & self.READ:
            self.handle_read()
        if events & self.WRITE:
            self.handle_write()
            
    def modify_state(self, current_state, state):
        if current_state != state:
            if current_state is None:
                self.poller.register(self.fd, state)
            else:
                self.poller.modify(self.fd, state)
                

class IOLoop(IObase):
    """\
A level-triggered I/O event loop adapted from tornado.

:parameter io: The I/O implementation. If not supplied, the best possible
    implementation available will be used. On posix system this is ``epoll``,
    or else ``select``. It can be any other custom implementation as long as
    it has an ``epoll`` like interface. Pulsar ships with an additional
    I/O implementation based on distributed queue :class:`IOQueue`.

**ATTRIBUTES**

.. attribute:: _impl

    The IO implementation

.. attribute:: cpubound

    If ``True`` this is a CPU bound event loop, otherwise it is an I/O
    event loop. CPU bound loops can block the loop for considerable amount
    of time.
        
.. attribute:: num_loops

    Total number of loops

.. attribute:: poll_timeout

    The timeout in seconds when polling with epol or select.

    Default: `0.5`

.. attribute:: tid

    The thread id where the eventloop is running
    
.. attribute:: tasks

    A list of callables to be executed at each iteration of the event loop.
    Task can be added and deleted via the :meth:`add_task` and
    :meth:`remove_task`. Extra care must be taken when adding tasks to
    I/O event loops. These tasks should be fast to perform and not block.

**METHODS**
"""
    # Never use an infinite timeout here - it can stall epoll
    poll_timeout = 0.5

    def __init__(self, io=None, logger=None, poll_timeout=None):
        self._impl = io or IOpoll()
        self.fd_factory = getattr(self._impl, 'fd_factory', FileDescriptor)
        self.poll_timeout = poll_timeout if poll_timeout else self.poll_timeout
        self.logger = logger or LOGGER
        if hasattr(self._impl, 'fileno'):
            close_on_exec(self._impl.fileno())
        self._handlers = {}
        self._callbacks = []
        self._timeouts = []
        self._started = None
        self._running = False
        self.num_loops = 0
        self._waker = getattr(self._impl, 'waker', Waker)()
        self.add_reader(self._waker, self._waker.consume)

    @property
    def cpubound(self):
        return getattr(self._impl, 'cpubound', False)

    def add_handler(self, fd, handler, events):
        """Registers the given *handler* to receive the given events for the
file descriptor *fd*.

:parameter fd: A file descriptor or an object with the ``fileno`` method.
:parameter handler: A callable which will be called when events occur on the
    file descriptor *fd*.
:rtype: ``True`` if the handler was succesfully added."""
        if fd is not None:
            fdd = file_descriptor(fd)
            if fdd not in self._handlers:
                self._handlers[fdd] = handler
                self._impl.register(fdd, events | self.ERROR)
                return True
            else:
                self.logger.debug('Handler for %s already available.', fd)

    def update_handler(self, fd, events):
        """Changes the events we listen for fd."""
        self._impl.modify(file_descriptor(fd), events | self.ERROR)

    def remove_handler(self, fd):
        """Stop listening for events on fd."""
        fdd = file_descriptor(fd)
        self._handlers.pop(fdd, None)
        self._events.pop(fdd, None)
        try:
            self._impl.unregister(fdd)
        except (OSError, IOError):
            self.logger.error("Error removing %s from IOLoop", fd, exc_info=True)
    
    @property
    def active(self):
        return self._callbacks or self._timeouts or self._handlers
    
    def run(self):
        '''Run the event loop until nothing left to do or stop() called.'''
        try:
            while self.active:
                try:
                    self._run_once()
                except StopEventLoop:
                    break
        finally:
            self._running = False

    def stop(self):
        '''Stop the loop after the current event loop iteration is complete.
If the event loop is not currently running, the next call to :meth:`start`
will return immediately.

To use asynchronous methods from otherwise-synchronous code (such as
unit tests), you can start and stop the event loop like this::

    ioloop = IOLoop()
    async_method(ioloop=ioloop, callback=ioloop.stop)
    ioloop.start()

:meth:`start` will return after async_method has run its callback,
whether that callback was invoked before or after ioloop.start.'''
        self.call_soon_threadsafe(_raise_stop_error)

    def running(self):
        """Returns true if this IOLoop is currently running."""
        return self._running

    def call_later(self, seconds, callback, *args):
        """Add a *callback* to be executed approximately *seconds* in the
future, once, unless cancelled. A timeout callback  it is called
at the time *deadline* from the :class:`IOLoop`.
It returns an handle that may be passed to remove_timeout to cancel."""
        if seconds > 0:
            timeout = TimedCall(timer() + seconds, callback, args,
                                self.remove_timeout)
            heapq.heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)

    def call_soon(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args, **kw)``.'''
        timeout = TimedCall(None, callback, args, self.remove_timeout)
        self._callbacks.append(timeout)
        return timeout
    
    def call_soon_threadsafe(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args, **kw)``.'''
        timeout = self.call_soon(callback, *args)
        self.wake()
        return timeout
    
    def add_timeout(self, deadline, callback):
        """Add a timeout *callback*. A timeout callback  it is called
at the time *deadline* from the :class:`IOLoop`.
It returns an handle that may be passed to remove_timeout to cancel."""
        timeout = _Timeout(deadline, callback)
        bisect.insort(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        """Cancels a pending *timeout*. The argument is an handle as returned
by the :meth:`add_timeout` method."""
        self._timeouts.remove(timeout)

    def add_callback(self, callback, wake=True):
        """Calls the given callback on the next I/O loop iteration.

        It is safe to call this method from any thread at any time.
        Note that this is the *only* method in IOLoop that makes this
        guarantee; all other interaction with the IOLoop must be done
        from that IOLoop's thread.  add_callback() may be used to transfer
        control from other threads to the IOLoop's thread.
        """
        self._callbacks.append(callback)
        if wake:
            self.wake()

    def add_periodic(self, callback, period):
        """Add a :class:`PeriodicCallback` to the event loop."""
        p = PeriodicCallback(callback, period, self)
        p.start()
        return p
        
    def add_reader(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = TimedCall(None, callback, args)
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].add_reader(callback)
        else:
            self._handlers[fd] = self.fd_factory(fd, self, read=handler)
        return handler
    
    def add_writer(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = TimedCall(None, callback, args, self)
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].add_writer(callback)
        else:
            self._handlers[fd] = self.fd_factory(fd, self, writer=handler)
        return handler
    
    def wake(self):
        '''Wake up the eventloop.'''
        if self.running():
            self._waker.wake()

    def create_server(self, sock, protocol, **params):
        pass
    
    ############################################################ INTERNALS    
    def _run_callback(self, callback, name='callback'):
        try:
            callback()
        except EXIT_EXCEPTIONS:
            raise
        except:
            self.logger.critical('Unhandled exception in %s.', name,
                                 exc_info=True)

    def _run(self):
        """Runs the I/O loop until one of the I/O handlers calls stop(), which
will make the loop stop after the current event iteration completes."""
        with LoopGuard(self) as guard:
            while self._running:
                poll_timeout = self.poll_timeout
                self.num_loops += 1
                _run_callback = self._run_callback
                # Prevent IO event starvation by delaying new callbacks
                # to the next iteration of the event loop.
                callbacks = self._callbacks
                if callbacks:
                    self._callbacks = []
                    for callback in callbacks:
                        _run_callback(callback)
                if self._timeouts:
                    now = time.time()
                    while self._timeouts and self._timeouts[0].deadline <= now:
                        timeout = self._timeouts.pop(0)
                        self._run_callback(timeout.callback)
                    if self._timeouts:
                        milliseconds = self._timeouts[0].deadline - now
                        poll_timeout = min(milliseconds, poll_timeout)
                # A chance to exit
                if not self._running:
                    break
                try:
                    event_pairs = self._impl.poll(poll_timeout)
                except Exception as e:
                    # Depending on python version and IOLoop implementation,
                    # different exception types may be thrown and there are
                    # two ways EINTR might be signaled:
                    # * e.errno == errno.EINTR
                    # * e.args is like (errno.EINTR, 'Interrupted system call')
                    eno = getattr(e, 'errno', None)
                    if eno != errno.EINTR:
                        args = getattr(e, 'args', None)
                        if isinstance(args, tuple) and len(args) == 2:
                            eno = args[0]
                    if eno != errno.EINTR and self._running:
                        raise
                    continue
                # Pop one fd at a time from the set of pending fds and run
                # its handler. Since that handler may perform actions on
                # other file descriptors, there may be reentrant calls to
                # this IOLoop that update self._events
                if event_pairs:
                    self._events.update(event_pairs)
                    _events = self._events
                    while _events:
                        fd, events = _events.popitem()
                        try:
                            self._handlers[fd](fd, events)
                        except EXIT_EXCEPTIONS:
                            raise
                        except (OSError, IOError) as e:
                            if e.args[0] == errno.EPIPE:
                                # Happens when the client closes the connection
                                pass
                            else:
                                self.logger.error(
                                    "Exception in I/O handler for fd %s",
                                              fd, exc_info=True)
                        except KeyError:
                            self.logger.info("File descriptor %s missing", fd)
                        except:
                            self.logger.error("Exception in I/O handler for fd %s",
                                          fd, exc_info=True)

    def _run_once(self, timeout=None):
        self._running = True
        setid(self)
        poll_timeout = self.poll_timeout
        self.num_loops += 1
        # Prevent IO event starvation by delaying new callbacks
        # to the next iteration of the event loop.
        callbacks = self._callbacks
        self._callbacks = []
        if self._timeouts:
            now = time.time()
            while self._timeouts and self._timeouts[0].deadline <= now:
                callbacks.append(self._timeouts.pop(0))
            if self._timeouts:
                seconds = self._timeouts[0].deadline - now
                poll_timeout = min(seconds, poll_timeout)
        try:
            event_pairs = self._impl.poll(poll_timeout)
        except Exception as e:
            # Depending on python version and IOLoop implementation,
            # different exception types may be thrown and there are
            # two ways EINTR might be signaled:
            # * e.errno == errno.EINTR
            # * e.args is like (errno.EINTR, 'Interrupted system call')
            eno = getattr(e, 'errno', None)
            if eno != errno.EINTR:
                args = getattr(e, 'args', None)
                if isinstance(args, tuple) and len(args) == 2:
                    eno = args[0]
            if eno != errno.EINTR and self._running:
                raise
        else:
            for fd, events in event_pairs:
                callback.append(partial(self._handlers[fd], events))
        for callback in callbacks:
            try:
                callback()
            except Exception:
                LOGGER.exception('Exception in callback %s', callback)
        
        

class _Timeout(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""
    __slots__ = ('deadline', 'callback')

    def __init__(self, deadline, callback):
        self.deadline = deadline
        self.callback = callback

    def __lt__(self, other):
        return ((self.deadline, id(self.callback)) <
                (other.deadline, id(other.callback)))


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every callback_time seconds.
    """
    def __init__(self, callback, callback_time, ioloop):
        self.callback = callback
        self.callback_time = callback_time
        self.ioloop = ioloop
        self._running = False

    def start(self):
        self._running = True
        timeout = time.time() + self.callback_time
        self.ioloop.add_timeout(timeout, self._run)

    def stop(self):
        self._running = False

    def _run(self):
        if not self._running:
            return
        try:
            self.callback(self)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            LOGGER.error("Error in periodic callback", exc_info=True)
        if self._running:
            self.start()


class _not_called_exception:

    def __init__(self, value):
        self.value = value

    def __call__(self):
        if not self.value.called:
            try:
                raise Timeout('"%s" timed out.' % self.value)
            except:
                self.value.callback(sys.exc_info())


def loop_timeout(value, timeout, ioloop=None):
    value = maybe_async(value)
    if timeout and is_async(value):
        ioloop = ioloop or thread_loop()
        return ioloop.add_timeout(time.time() + timeout,
                                  _not_called_exception(value))

