import os
import sys
import heapq
import logging
import traceback
import inspect
import errno
import socket
from functools import partial
from threading import current_thread
try:
    import signal
except ImportError: #pragma    nocover
    signal = None

from pulsar.utils.system import IObase, IOpoll, close_on_exec, platform, Waker
from pulsar.utils.pep import default_timer, set_event_loop_policy,\
                             set_event_loop, new_event_loop, get_event_loop,\
                             EventLoop as BaseEventLoop,\
                             EventLoopPolicy as BaseEventLoopPolicy
from pulsar.utils.sockets import SOCKET_INTERRUPT_ERRORS

from .access import thread_local_data
from .defer import log_failure, is_failure, Deferred, TimeoutError, is_async
from .transports import create_server

__all__ = ['EventLoop', 'TimedCall']

LOGGER = logging.getLogger('pulsar.eventloop')


def file_descriptor(fd):
    if hasattr(fd, 'fileno'):
        return fd.fileno()
    else:
        return fd

def setid(self):
    ct = current_thread()
    self.tid = ct.ident
    self.pid = os.getpid()
    return ct


class StopEventLoop(BaseException):
    """Raised to stop the event loop."""
    
    
class EventLoopPolicy(BaseEventLoopPolicy):
    '''Pulsar event loop policy'''
    def get_event_loop(self):
        return thread_local_data('_event_loop')
    
    def get_request_loop(self):
        return thread_local_data('_request_loop') or self.get_event_loop()
    
    def new_event_loop(self, **kwargs):
        return EventLoop(**kwargs)
    
    def set_event_loop(self, event_loop):
        """Set the event loop."""
        assert event_loop is None or isinstance(event_loop, BaseEventLoop)
        if getattr(event_loop, 'cpubound', False):
            thread_local_data('_request_loop', event_loop)
        else:
            thread_local_data('_event_loop', event_loop)
        
    
set_event_loop_policy(EventLoopPolicy())
                
            
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

    def __init__(self, deadline, callback, args):
        self.reschedule(deadline)
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
        self._cancelled = True
                
    def reschedule(self, new_deadline):
        self._deadline = new_deadline
        self._cancelled = False
    
    def __call__(self, *args, **kwargs):
        if not self._cancelled:
            args = self._args + args
            return self._callback(*args, **kwargs)
        
        
class FileDescriptor(IObase):
    def __init__(self, fd, eventloop):
        self.fd = fd
        self.eventloop = eventloop
        self.handle_write = None
        self.handle_read = None

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
        self.add_writer(callback, 'connector')
        
    def add_reader(self, callback):
        handle_read = self.handle_read
        if handle_read:
            if handle_read.callback != callback.callback:
                raise RuntimeError('Read handler already registered')
        else:
            LOGGER.debug('Add reader on file descriptor %s', self.fd)
            current_state = self.state
            self.handle_read = callback
            self.modify_state(current_state, self.READ)
        
    def add_writer(self, callback, name=None):
        name = name or 'writer'
        handle_write = self.handle_write
        if handle_write:
            if handle_write.callback != callback.callback:
                raise RuntimeError('%s handler already registered', name)
        else:
            LOGGER.debug('Add %s on file descriptor %s', name, self.fd)
            current_state = self.state
            self.handle_write = callback
            self.modify_state(current_state, self.WRITE)
        
    def remove_connector(self):
        return self.remove_writer('connector')
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        if self.writing:
            LOGGER.debug('Remove reader from file descriptor %s', self.fd)
            self.poller.modify(self.fd, self.WRITE)
        else:
            self.remove_handler()
        self.handle_read = None

    def remove_writer(self, name=None):
        '''Remove writer and return True if reading'''
        if self.reading:
            LOGGER.debug('Remove %s from file descriptor %s',
                         name or 'writer', self.fd)
            self.poller.modify(self.fd, self.READ)
        else:
            self.remove_handler()
        self.handle_write = None
    
    def __call__(self, events):
        if events & self.READ:
            if self.handle_read:
                log_failure(self.handle_read())
            else:
                LOGGER.warning('Read callback without handler for file'
                               ' descriptor %s.', self.fd)
        if events & self.WRITE:
            if self.handle_write:
                log_failure(self.handle_write())
            else:
                LOGGER.warning('Write callback without handler for file'
                               ' descriptor %s.', self.fd)
            
    def modify_state(self, current_state, state):
        if current_state != state:
            if current_state is None:
                self.poller.register(self.fd, state)
            else:
                self.poller.modify(self.fd, current_state | state)
    
    def remove_handler(self):
        """Stop listening for events on fd."""
        fd = self.fd
        try:
            self.poller.unregister(fd)
        except (OSError, IOError):
            self.eventloop.logger.error("Error removing %s from EventLoop", fd)
        LOGGER.debug('Remove file descriptor %s', fd)
        self.eventloop._handlers.pop(fd, None)


class EventLoop(IObase, BaseEventLoop):
    """A pluggable event loop which conforms with the pep-3156_ API. The
event loop is the place where most asynchronous operations are carried out.

.. attribute:: io

    The I/O implementation. If not supplied, the best possible
    implementation available will be used. On posix system this is ``epoll``,
    or else ``select``. It can be any other custom implementation as long as
    it has an ``epoll`` like interface.

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

"""
    # Never use an infinite timeout here - it can stall epoll
    poll_timeout = 0.5

    def __init__(self, io=None, logger=None, poll_timeout=None, timer=None,
                 iothreadloop=True):
        self._impl = io or IOpoll()
        self.fd_factory = getattr(self._impl, 'fd_factory', FileDescriptor)
        self.timer = timer or default_timer
        self.poll_timeout = poll_timeout if poll_timeout else self.poll_timeout
        self.logger = logger or LOGGER
        close_on_exec(self._impl.fileno())
        self._iothreadloop = iothreadloop
        self._handlers = {}
        self._callbacks = []
        self._scheduled = []
        self._name = None
        self.num_loops = 0
        self._waker = self.install_waker()
    
    def __repr__(self):
        return self.name
    __str__ = __repr__
    
    @property
    def name(self):
        name = self._name if self._name else '<not running>'
        if self.cpubound:
            return 'CPU bound %s %s' % (self.__class__.__name__, name)
        else:
            return '%s %s' % (self.__class__.__name__, name)
            
    @property
    def io(self):
        return self._impl
    
    @property
    def iothreadloop(self):
        '''``True`` if this :class:`EventLoop` install itself as the event
loop of the thread where it is run.'''
        return self._iothreadloop
    
    @property
    def cpubound(self):
        return getattr(self._impl, 'cpubound', False)
    
    @property
    def running(self):
        return bool(self._name)
    
    @property
    def active(self):
        return bool(self._callbacks or self._scheduled or self._handlers)
    
    ############################################################################
    ##    PEP 3156 Methods
    def run(self):
        '''Run the event loop until nothing left to do or stop() called.'''
        if not self.running:
            self._before_run()
            try:
                while self.active:
                    try:
                        self._run_once()
                    except StopEventLoop:
                        break
            finally:
                self._after_run()
    
    def run_forever(self):
        '''Run the event loop forever.'''
        if not self.running:
            self._before_run()
            try:
                while True:
                    try:
                        self._run_once()
                    except StopEventLoop:
                        break
            finally:
                self._after_run()

    def run_once(self, timeout=None):
        """Run through all callbacks and all I/O polls once.

        Calling stop() will break out of this too.
        """
        if not self.running:
            self._before_run()
            try:
                try:
                    self._run_once(timeout)
                except StopEventLoop:
                    pass
            finally:
                self._after_run()
                
    def run_until_complete(self, future, timeout=None):
        '''Run the event loop until a :class:`Deferred` *future* is done.
Return the future's result, or raise its exception. If timeout is not
``None``, run it for at most that long;  if the future is still not done,
raise TimeoutError (but don't cancel the future).'''
        self.call_soon(future.add_both, self._raise_stop_event_loop)
        handler = None
        if timeout:
            handler = self.call_later(timeout, self._raise_stop_event_loop)
        self.run()
        if handler:
            if future.done():
                handler.cancel()
            else:
                raise TimeoutError
        result = future.result_or_self()
        if is_failure(result):
            result.raise_all()
        else:
            return result
        
    def stop(self):
        '''Stop the loop after the current event loop iteration is complete'''
        self.call_soon_threadsafe(self._raise_stop_event_loop)
    
    def call_at(self, when, callback, *args):
        if when > self.timer():
            timeout = TimedCall(when, callback, args)
            heapq.heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)
        
    def call_later(self, seconds, callback, *args):
        """Arrange for a *callback* to be called at a given time in the future.
Return an :class:`TimedCall` with a :meth:`TimedCall.cancel` method
that can be used to
cancel the call. The delay can be an int or float, expressed in
seconds.  It is always a relative time.

Each callback will be called exactly once.  If two callbacks
are scheduled for exactly the same time, it is undefined which
will be called first.

Callbacks scheduled in the past are passed on to :meth:`call_soon` method,
so these will be called in the order in which they were
registered rather than by time due.  This is so you can't
cheat and insert yourself at the front of the ready queue by
using a negative time.

Any positional arguments after the callback will be passed to
the callback when it is called."""
        if seconds > 0:
            timeout = TimedCall(self.timer() + seconds, callback, args)
            heapq.heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)
        
    def call_soon(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args)``.'''
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
        """Call a *callback* every *interval* seconds."""
        if interval > 0:
            def wrapper():
                callback(*args)  # If this fails, the chain is broken.
                handler.reschedule(self.timer() + interval)
                heapq.heappush(self._scheduled, handler)
            handler = self.call_later(interval, wrapper)
            return handler
        else:
            return self.call_every(callback, *args)
    
    def call_every(self, callback, *args):
        """Call a *callback* at every loop. If the return value form the
callback is asynchronous, resume the callback once the result is done.
If an error occur in the *callback*, the chain is broken and the *callback*
won't be called anymore."""
        def wrapper():
            result = callback(*args)  # If this fails, the chain is broken.
            if is_async(result):
                result.add_callback(lambda r: self._callbacks.append(handler),
                                    log_failure)
            elif is_failure(result):
                log_failure(result)
            else:
                self._callbacks.append(handler)
        handler = self.call_soon(wrapper)
        return handler
        
    def add_reader(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = TimedCall(None, callback, args)
        fd = file_descriptor(fd)
        if fd not in self._handlers:
            self._handlers[fd] = self.fd_factory(fd, self)
        self._handlers[fd].add_reader(handler)
        return handler
    
    def add_writer(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = TimedCall(None, callback, args)
        fd = file_descriptor(fd)
        if fd not in self._handlers:
            self._handlers[fd] = self.fd_factory(fd, self)
        self._handlers[fd].add_writer(handler)
        return handler
    
    def add_connector(self, fd, callback, *args):
        handler = TimedCall(None, callback, args)
        fd = file_descriptor(fd)
        if fd not in self._handlers:
            self._handlers[fd] = self.fd_factory(fd, self)
        self._handlers[fd].add_connector(handler)
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
            
    def remove_connector(self, fd):
        fd = file_descriptor(fd)
        if fd in self._handlers:
            self._handlers[fd].remove_connector()
    
    def add_signal_handler(self, sig, callback, *args):
        '''Whenever signal ``sig`` is received, arrange for `callback(*args)` to
be called. Returns a :class:`TimedCall` handler which can be used to cancel
the signal callback.'''
        self._check_signal(sig)
        handler = TimedCall(None, callback, args)
        prev = signal.signal(sig, handler)
        if isinstance(prev, TimedCall):
            prev.cancel()
        return handler
    
    def remove_signal_handler(self, sig):
        '''Remove the signal ``sig`` if it was installed and reinstal the
default signal handler ``signal.SIG_DFL``.'''
        self._check_signal(sig)
        handler = signal.signal(sig, signal.SIG_DFL)
        if handler:
            handler.cancel()
            return True
        else:
            return False
        
    def create_server(self, **kwargs):
        '''Create a new :class:`Server`.'''
        if self.cpubound:
            raise RuntimeError('Cannot create server from a cpubound eventloop')
        return create_server(self, **kwargs)
    
    def wake(self):
        '''Wake up the eventloop.'''
        if self.running and self._waker:
            self._waker.wake()
            
    ############################################################ NON PEP METHODS        
    def call_now_threadsafe(self, callback, *args):
        if self.tid != current_thread().ident:
            return self.call_soon_threadsafe(callback, *args)
        else:
            self._call(callback, *args)
            
    def has_callback(self, callback):
        if callback.deadline:
            return callback in self._scheduled
        else:
            return callback in self._callbacks
        
    def install_waker(self):
        # Install event loop wake if possible
        if hasattr(self._impl, 'install_waker'):
            return self._impl.install_waker(self)
        else:
            waker = Waker()
            self.add_reader(waker, waker.consume)
            return waker

    ############################################################ INTERNALS
    def _before_run(self):
        ct = setid(self)
        self._name = ct.name
        if self._iothreadloop:
            set_event_loop(self)
    
    def _after_run(self):
        self.logger.debug('Exiting %s', self)
        self._name = None
        self.tid = None
        
    def _raise_stop_event_loop(self, exc=None):
        self.logger.debug('Stopping %s', self)
        raise StopEventLoop

    def _check_signal(self, sig):
        """Internal helper to validate a signal.

        Raise ValueError if the signal number is invalid or uncatchable.
        Raise RuntimeError if there is a problem setting up the handler.
        """
        if not isinstance(sig, int):
            raise TypeError('sig must be an int, not {!r}'.format(sig))
        if signal is None:
            raise RuntimeError('Signals are not supported')
        if not (1 <= sig < signal.NSIG):
            raise ValueError('sig {} out of range(1, {})'.format(sig,
                                                                 signal.NSIG))

    def _run_once(self, timeout=None):
        poll_timeout = timeout or self.poll_timeout
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
            if self._raise_loop_error(e):
                raise
        except KeyboardInterrupt as e:
            LOGGER.warning('%s stop event loop', e.__class__.__name__)
            raise StopEventLoop
        else:
            for fd, events in event_pairs:
                if fd in self._handlers:
                    callbacks.append(partial(self._handlers[fd], events))
                else:
                    LOGGER.warning('Received an event on unregistered file '\
                                   'descriptor %s' % fd)
        call = self._call
        for callback in callbacks:
            call(callback)
            
    def _call(self, callback, *args):
        try:
            log_failure(callback(*args))
        except socket.error as e:
            if self._raise_loop_error(e):
                log_failure(e, msg='Exception in event loop callback.')
        except Exception as e:
            log_failure(e, msg='Exception in event loop callback.')

    def _raise_loop_error(self, e):
        # Depending on python version and EventLoop implementation,
        # different exception types may be thrown and there are
        # two ways EINTR might be signaled:
        # * e.errno == errno.EINTR
        # * e.args is like (errno.EINTR, 'Interrupted system call')
        eno = getattr(e, 'errno', None)
        if eno not in SOCKET_INTERRUPT_ERRORS:
            args = getattr(e, 'args', None)
            if isinstance(args, tuple) and len(args) == 2:
                eno = args[0]
        if eno not in SOCKET_INTERRUPT_ERRORS and self.running:
            return True