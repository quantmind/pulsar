import os
import sys
import heapq
import logging
import traceback
import inspect
import signal
import errno
import socket
from functools import partial
from threading import current_thread

from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform, Waker
from pulsar.utils.pep import default_timer, DefaultEventLoopPolicy,\
                             set_event_loop_policy, set_event_loop,\
                             get_event_loop, EventLoop as BaseEventLoop
from .defer import log_failure, Deferred
from .servers import Server

__all__ = ['EventLoop', 'TimedCall', 'asynchronous']

LOGGER = logging.getLogger('pulsar.eventloop')

    
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


class EventLoopPolicy(DefaultEventLoopPolicy):
    _request_loop = None
    def get_event_loop(self):
        return self._event_loop
    
    def get_request_loop(self):
        return self._request_loop or self._event_loop
    
    def new_event_loop(self, **kwargs):
        return EventLoop(**kwargs)
    
    def set_event_loop(self, event_loop):
        """Set the event loop."""
        assert event_loop is None or isinstance(event_loop, BaseEventLoop)
        if event_loop.cpubound:
            self._request_loop = event_loop
        else:
            self._event_loop = event_loop
    
    
set_event_loop_policy(EventLoopPolicy())


class asynchronous:
    
    def __call__(self, f):
        assert inspect.isgeneratorfunction(f), 'required generator function'
        self.func = f
        def _(*args, **kwargs):
            d = Deferred()
            self.generate(get_event_loop(), d.callback, f(*args, **kwargs))
            return d
        return _
    
    def generate(self, eventloop, callback, gen, value=None):
        try:
            value = next(gen)
            eventloop.call_soon_threadsafe(self.generate, eventloop,
                                           callback, gen, value)
        except StopIteration:
            callback(value)
            pass
        except Exception as e:
            callback(e)
                
            
            
class TimedCall(object):
    """An EventLoop callback handler. This is not initialised directly, instead
it is created by :meth:`EventLoop.call_soon`, :meth:`EventLoop.call_later`,
:meth:`EventLoop.call_soon_threadsafe` and so forth.
    
.. attribute:: deadline

    a time in the future or ``None``.
    
.. attribute:: callback

    The callback to execute in the eventloop
    
.. attribute:: cancelled

    Flag indicating this callback is cancelled.
    """

    def __init__(self, deadline, callback, args, canceller=None):
        self.reschedule(deadline)
        self._canceller = canceller
        self._callback = callback
        self._args = args

    def __lt__(self, other):
        return self.deadline < other.deadline
        
    @property
    def deadline(self):
        return self._deadline
    
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
        if not self._cancelled:
            self._cancelled = True
            if self._canceller:
                self._canceller(self)
                
    def reschedule(self, new_deadline):
        self._deadline = new_deadline
        self._cancelled = False
    
    def __call__(self, *args, **kwargs):
        if not self._cancelled:
            if self._deadline:  # cancel of if a deadline !important
                self._cancelled = True
            self._callback(*self.args)
        
        
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
        return bool(self.handle_write)
    
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
        if self.reading:
            raise RuntimeError('Read handler already registered')
        else:
            current_state = self.state
            self.handle_read = callback
            self.modify_state(current_state, self.READ)
        
    def add_writer(self, callback):
        if self.writing:
            raise RuntimeError('Write handler already registered')
        else:
            current_state = self.state
            self.handle_write = callback
            self.modify_state(current_state, self.WRITE)
        
    def remove_connector(self):
        self._connecting = False
        return self.remove_writer()
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        if self.writing:
            self.modify_state(self.state, self.WRITE)
        else:
            self.remove_handler()
        self.handle_read = None

    def remove_writer(self):
        '''Remove writer and return True if reading'''
        if self.reading:
            self.modify_state(self.state, self.READ)
        else:
            self.remove_handler()
        self.handle_write = None
    
    def __call__(self, events):
        if events & self.READ:
            if self.handle_read:
                self.handle_read()
        if events & self.WRITE:
            if self.handle_write:
                self.handle_write()
            
    def modify_state(self, current_state, state):
        if current_state != state:
            if current_state is None:
                self.poller.register(self.fd, state)
            else:
                self.poller.modify(self.fd, state)
    
    def remove_handler(self):
        """Stop listening for events on fd."""
        fd = self.fd
        try:
            self.poller.unregister(fd)
        except (OSError, IOError):
            self.eventloop.logger.error("Error removing %s from EventLoop", fd)
        self.eventloop._handlers.pop(fd, None)


class EventLoop(IObase, BaseEventLoop):
    """A pluggable event loop which conforms with the pep-3156_ API. The
event loop is the place where most asynchronous operations are carried out.

**ATTRIBUTES**

.. attribute:: io

    The I/O implementation. If not supplied, the best possible
    implementation available will be used. On posix system this is ``epoll``,
    or else ``select``. It can be any other custom implementation as long as
    it has an ``epoll`` like interface. Pulsar ships with an additional
    I/O implementation based on distributed queue :class:`IOQueue`.

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

**METHODS**
"""
    # Never use an infinite timeout here - it can stall epoll
    poll_timeout = 0.5

    def __init__(self, io=None, logger=None, poll_timeout=None, timer=None):
        self._impl = io or IOpoll()
        self.fd_factory = getattr(self._impl, 'fd_factory', FileDescriptor)
        self.timer = timer or default_timer
        self.poll_timeout = poll_timeout if poll_timeout else self.poll_timeout
        self.logger = logger or LOGGER
        if hasattr(self._impl, 'fileno'):
            close_on_exec(self._impl.fileno())
        self._handlers = {}
        self._signals = {}
        self._callbacks = []
        self._scheduled = []
        self._started = None
        self._running = False
        self.num_loops = 0
        self._waker = getattr(self._impl, 'waker', Waker)()
        self.add_reader(self._waker, self._waker.consume)

    @property
    def io(self):
        return self._impl
    
    @property
    def cpubound(self):
        return getattr(self._impl, 'cpubound', False)
    
    @property
    def running(self):
        return self._running
    
    @property
    def active(self):
        return bool(self._callbacks or self._scheduled or self._handlers)
    
    def run(self):
        '''Run the event loop until nothing left to do or stop() called.'''
        if not self._running:
            set_event_loop(self)
            try:
                while self.active:
                    try:
                        self._run_once()
                    except StopEventLoop:
                        break
            finally:
                self._running = False

    def run_once(self, timeout=None):
        """Run through all callbacks and all I/O polls once.

        Calling stop() will break out of this too.
        """
        if not self._running:
            set_event_loop(self)
            try:
                try:
                    self._run_once(timeout)
                except StopEventLoop:
                    pass
            finally:
                self._running = False
        
    def stop(self):
        '''Stop the loop after the current event loop iteration is complete'''
        self.call_soon_threadsafe(_raise_stop_event_loop)
        
    def call_later(self, seconds, callback, *args):
        """Arrange for a *callback* to be called at a given time in the future.
Return an :class:`TimedCall` with a :meth:`TimedCall.cancel' method
that can be used to
cancel the call. The delay can be an int or float, expressed in
seconds.  It is always a relative time.

Each callback will be called exactly once.  If two callbacks
are scheduled for exactly the same time, it is undefined which
will be called first.

Callbacks scheduled in the past are passed on to call_soon(),
so these will be called in the order in which they were
registered rather than by time due.  This is so you can't
cheat and insert yourself at the front of the ready queue by
using a negative time.

Any positional arguments after the callback will be passed to
the callback when it is called."""
        if seconds > 0:
            timeout = TimedCall(self.timer() + seconds, callback, args,
                                self._remove_timeout)
            heapq.heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)
        
    def call_soon(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args, **kw)``.'''
        timeout = TimedCall(None, callback, args)
        self._callbacks.append(timeout)
        return timeout
    
    def call_soon_threadsafe(self, callback, *args):
        '''Calls the given callback on the next I/O loop iteration.
It is safe to call this method from any thread at any time.
Note that this is the *only* method in :class:`EventLoop` that
makes this guarantee. all other interaction with the :class:`EventLoop`
must be done from that :class:`EventLoop`'s thread. It may be used
to transfer control from other threads to the EventLoop's thread.'''
        timeout = self.call_soon(callback, *args)
        self.wake()
        return timeout

    def call_repeatedly(self, interval, callback, *args):
        """Call a *callback* every *interval* seconds.
        
**TODO: be able to cancel it.**"""
        def wrapper():
            callback(*args)  # If this fails, the chain is broken.
            handler.reschedule(self.timer() + interval)
            heapq.heappush(self._scheduled, handler)
        handler = TimedCall(interval, wrapper, (), self._remove_timeout)
        heapq.heappush(self._scheduled, handler)
        return handler
        
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
        handler = TimedCall(None, callback, args)
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].add_writer(callback)
        else:
            self._handlers[fd] = self.fd_factory(fd, self, write=handler)
        return handler
    
    def remove_reader(self, fd):
        '''Cancels the current read callback for file descriptor fd,
if one is set. A no-op if no callback is currently set for the file
descriptor.'''
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].remove_reader()
    
    def remove_writer(self, fd):
        '''Cancels the current write callback for file descriptor fd,
if one is set. A no-op if no callback is currently set for the file
descriptor.'''
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].remove_writer()
    
    def add_signal_handler(self, sig, callback, *args):
        '''Whenever signal ``sig`` is received, arrange for callback(*args) to
be called. Returns a :class:`TimedCall` handler which can be used to cancel
the signal callback.'''
        handler = TimedCall(None, callback, args)
        prev = signal.signal(sig, handler)
        if isinstance(prev, TimedCall):
            prev.cancel()
        return handler
    
    def remove_signal_handler(self, sig):
        '''Remove the signal ``sig`` if it was installed and reinstal the
default signal handler ``signal.SIG_DFL``.'''
        handler = signal.signal(sig, signal.SIG_DFL)
        if handler:
            handler.cancel()
            return True
        else:
            return False
        
    def create_server(self, **kwargs):
        '''Create a new :class:`Server`.'''
        kwargs['eventloop'] = self
        return Server.create(**kwargs)
    
    def wake(self):
        '''Wake up the eventloop.'''
        if self.running:
            self._waker.wake()

    ############################################################ INTERNALS
    def _remove_timeout(self, timeout):
        try:
            self._scheduled.remove(timeout)
        except ValueError:
            LOGGER.warn('trying to remove a timeout not scheduled.')

    def _run_once(self, timeout=None):
        self._running = True
        poll_timeout = timeout or self.poll_timeout
        setid(self)
        self.num_loops += 1
        # Prevent IO event starvation by delaying new callbacks
        # to the next iteration of the event loop.
        callbacks = self._callbacks
        self._callbacks = []
        if self._scheduled:
            now = self.timer()
            while self._scheduled and self._scheduled[0].deadline <= now:
                callbacks.append(self._scheduled.pop(0))
            if self._scheduled:
                seconds = self._scheduled[0].deadline - now
                poll_timeout = min(seconds, poll_timeout)
        try:
            event_pairs = self._impl.poll(poll_timeout)
        except Exception as e:
            # Depending on python version and EventLoop implementation,
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
                if fd in self._handlers:
                    callbacks.append(partial(self._handlers[fd], events))
                else:
                    LOGGER.warning('Received an event on unregistered file '\
                                   'descriptor %s' % fd)
        for callback in callbacks:
            try:
                log_failure(callback())
            except Exception:
                LOGGER.exception('Exception in event loop callback.')

