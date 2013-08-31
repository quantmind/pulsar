import os
import sys
import socket
from heapq import heappush, heappop
from functools import partial
from collections import deque
from threading import current_thread
try:
    import signal
except ImportError: #pragma    nocover
    signal = None

from pulsar.utils.system import close_on_exec
from pulsar.utils.pep import (default_timer, set_event_loop_policy,
                              set_event_loop, range,
                              EventLoop as BaseEventLoop, 
                              EventLoopPolicy as BaseEventLoopPolicy)
from pulsar.utils.internet import SOCKET_INTERRUPT_ERRORS
from pulsar.utils.exceptions import StopEventLoop, ImproperlyConfigured

from .access import thread_local_data, LOGGER
from .defer import Failure, TimeoutError, maybe_async
from .stream import create_connection, start_serving, sock_connect, sock_accept
from .udp import create_datagram_endpoint
from .consts import DEFAULT_CONNECT_TIMEOUT, DEFAULT_ACCEPT_TIMEOUT
from .pollers import DefaultIO

__all__ = ['EventLoop', 'TimedCall']


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

STOP_LOOP = (KeyboardInterrupt, )
    
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
        

class LoopingCall(object):
    
    def __init__(self, event_loop, callback, args, interval=None):
        self.event_loop = event_loop
        self.callback = callback
        self.args = args
        self._cancelled = False
        interval = interval or 0
        if interval > 0:
            self.interval = interval
            self.handler = self.event_loop.call_later(interval, self)
        else:
            self.interval = None
            self.handler = self.event_loop.call_soon(self)
    
    @property
    def cancelled(self):
        return self._cancelled
    
    def cancel(self, result=None):
        '''Attempt to cancel the callback.'''
        self._cancelled = True
        
    def __call__(self):
        try:
            result = maybe_async(self.callback(*self.args),
                                 get_result=False,
                                 event_loop=self.event_loop)
        except Exception:
            result = Failure(sys.exc_info())
            self.cancel(result)
        else:
            result.add_callback(self._continue, self.cancel)
        
    def _continue(self, result):
        if not self._cancelled:
            handler = self.handler
            event_loop = self.event_loop
            if self.interval:
                handler.reschedule(event_loop.timer() + self.interval)
                heappush(event_loop._scheduled, handler)
            else:
                self.event_loop._callbacks.append(self.handler)
                

class EventLoop(BaseEventLoop):
    """A pluggable event loop which conforms with the pep-3156_ API. The
event loop is the place where most asynchronous operations are carried out.

.. attribute:: poll_timeout

    The timeout in seconds when polling with ``epolL``, ``kqueue``, ``select``
    and so forth.

    Default: ``0.5``

.. attribute:: tid

    The thread id where this event loop is running. If the event loop is not
    running this attribute is ``None``.

"""
    # Never use an infinite timeout here - it can stall epoll
    poll_timeout = 0.5
    tid = None
    pid = None

    def __init__(self, io=None, logger=None, poll_timeout=None, timer=None,
                 iothreadloop=True):
        self._io = io or DefaultIO()
        self.timer = timer or default_timer
        self.poll_timeout = poll_timeout if poll_timeout else self.poll_timeout
        self.logger = logger or LOGGER
        close_on_exec(self._io.fileno())
        self._iothreadloop = iothreadloop
        self._handlers = {}
        self._callbacks = deque()
        self._scheduled = []
        self._name = None
        self._num_loops = 0
        self._default_executor = None
        self._waker = self._io.install_waker(self)
    
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
        '''The :class:`Poller` for this event loop. If not supplied,
        the best possible implementation available will be used. On posix
        system this is ``epoll`` or ``kqueue`` (Mac OS)
        or else ``select``.'''
        return self._io
    
    @property
    def iothreadloop(self):
        '''``True`` if this :class:`EventLoop` install itself as the event
loop of the thread where it is run.'''
        return self._iothreadloop
    
    @property
    def cpubound(self):
        '''If ``True`` this is a CPU bound event loop, otherwise it is an I/O
        event loop. CPU bound loops can block the loop for considerable amount
        of time.'''
        return getattr(self._io, 'cpubound', False)
    
    @property
    def running(self):
        return bool(self._name)
    
    @property
    def active(self):
        return bool(self._callbacks or self._scheduled or self._handlers)
    
    @property
    def num_loops(self):
        '''Total number of loops.'''
        return self._num_loops
    
    #################################################    STARTING & STOPPING
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
        result = future.result
        if isinstance(result, Failure):
            result.throw()
        else:
            return result
    
    def stop(self):
        '''Stop the loop after the current event loop iteration is complete'''
        self.call_soon_threadsafe(self._raise_stop_event_loop)
    
    def is_running(self):
        '''``True`` if the loop is running.'''
        return bool(self._name)
    
    #################################################    CALLBACKS
    def call_at(self, when, callback, *args):
        '''Arrange for a ``callback`` to be called at a given time ``when``
in the future. The time is an absolute time, for relative time check
the :meth:`call_later` method.
Returns a :class:`TimedCall` with a :meth:`TimedCall.cancel` method
that can be used to cancel the call.'''
        if when > self.timer():
            timeout = TimedCall(when, callback, args)
            heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)
        
    def call_later(self, seconds, callback, *args):
        '''Arrange for a ``callback`` to be called at a given time in the future.
Returns a :class:`TimedCall` with a :meth:`TimedCall.cancel` method
that can be used to cancel the call. The delay can be an int or float,
expressed in ``seconds``. It is always a relative time.

Each callback will be called exactly once.  If two callbacks
are scheduled for exactly the same time, it is undefined which
will be called first.

Callbacks scheduled in the past are passed on to :meth:`call_soon` method,
so these will be called in the order in which they were
registered rather than by time due.  This is so you can't
cheat and insert yourself at the front of the ready queue by
using a negative time.

Any positional arguments after the callback will be passed to
the callback when it is called.'''
        if seconds > 0:
            timeout = TimedCall(self.timer() + seconds, callback, args)
            heappush(self._scheduled, timeout)
            return timeout
        else:
            return self.call_soon(callback, *args)
        
    def call_soon(self, callback, *args):
        '''Equivalent to ``self.call_later(0, callback, *args)``.'''
        timeout = TimedCall(None, callback, args)
        self._callbacks.append(timeout)
        return timeout
    
    #################################################    THREAD INTERACTION
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
    
    def run_in_executor(self, executor, callback, *args):
        '''Arrange to call ``callback(*args)`` in an ``executor``.
        
        Return a :class:`Deferred` called once the callback has finished.'''
        executor = executor or self._default_executor
        if executor is None:
            raise ImproperlyConfigured('No executor available')
        return executor.apply(callback, *args)
        
    def set_default_executor(self, executor):
        self._default_executor = executor
    
    #################################################    INTERNET NAME LOOKUPS
    def getaddrinfo(self, host, port, family=0, type=0, proto=0, flags=0):
        return socket.getaddrinfo(host, port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return socket.getnameinfo(sockaddr, flags)
    
    #################################################    I/O CALLBACKS
    def add_reader(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = TimedCall(None, callback, args)
        self._io.add_reader(file_descriptor(fd), handler)
        return handler
    
    def add_writer(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = TimedCall(None, callback, args)
        self._io.add_writer(file_descriptor(fd), handler)
        return handler
    
    def add_connector(self, fd, callback, *args):
        handler = TimedCall(None, callback, args)
        fd = file_descriptor(fd)
        self._io.add_writer(fd, handler)
        self._io.add_error(fd, handler)
        return handler
    
    def remove_reader(self, fd):
        '''Cancels the current read callback for file descriptor fd,
if one is set. A no-op if no callback is currently set for the file
descriptor.'''
        return self._io.remove_reader(file_descriptor(fd))
    
    def remove_writer(self, fd):
        '''Cancels the current write callback for file descriptor fd,
if one is set. A no-op if no callback is currently set for the file
descriptor.'''
        return self._io.remove_writer(file_descriptor(fd))
            
    def remove_connector(self, fd):
        fd = file_descriptor(fd)
        w = self._io.remove_writer(fd)
        e = self._io.remove_error(fd)
        return w or e
    
    #################################################    SIGNAL CALLBACKS
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
    
    #################################################    SOCKET METHODS        
    def create_connection(self, protocol_factory, host=None, port=None,
                          ssl=None, family=0, proto=0, flags=0, sock=None,
                          local_addr=None, timeout=None):
        '''Creates a stream connection to a given internet host and port.
        
It is the asynchronous equivalent of ``socket.create_connection``.

:parameter protocol_factory: The callable to create the :class:`Protocol`
    which handle the connection.
:parameter host: If host is an empty string or None all interfaces are assumed
    and a list of multiple sockets will be returned (most likely
    one for IPv4 and another one for IPv6)
:parameter port:
:parameter ssl:
:parameter family:
:parameter proto:
:parameter flags:
:parameter sock:
:parameter local_addr: if supplied, it must be a 2-tuple ``(host, port)`` for
    the socket to bind to as its source address before connecting.
:return: a :class:`Deferred` and its result on success is the
    ``(transport, protocol)`` pair.
    
If a failure prevents the creation of a successful connection, an appropriate
exception will be raised.'''
        timeout = timeout or DEFAULT_CONNECT_TIMEOUT
        res = create_connection(self, protocol_factory, host, port,
                                ssl, family, proto, flags, sock, local_addr)
        return maybe_async(res, event_loop=self, timeout=timeout,
                           get_result=False)
    
    def start_serving(self, protocol_factory, host=None, port=None, ssl=None,
                      family=socket.AF_UNSPEC, flags=socket.AI_PASSIVE,
                      sock=None, backlog=100, reuse_address=None):
        """Creates a TCP server bound to ``host`` and ``port``.
        
:parameter protocol_factory: The :class:`Protocol` which handle server requests.
:parameter host: If host is an empty string or None all interfaces are assumed
    and a list of multiple sockets will be returned (most likely
    one for IPv4 and another one for IPv6).
:parameter port: integer indicating the port number.
:parameter ssl: can be set to an SSLContext to enable SSL over the accepted
    connections.
:parameter family: socket family can be set to either ``AF_INET`` or
    ``AF_INET6`` to force the socket to use IPv4 or IPv6.
    If not set it will be determined from host (defaults to ``AF_UNSPEC``).
:parameter flags: is a bitmask for :meth:`getaddrinfo`.
:parameter sock: can optionally be specified in order to use a pre-existing
    socket object.
:parameter backlog: is the maximum number of queued connections passed to
    listen() (defaults to 100).
:parameter reuse_address: tells the kernel to reuse a local socket in
    ``TIME_WAIT`` state, without waiting for its natural timeout to
    expire. If not specified will automatically be set to ``True`` on UNIX.
:return: a :class:`Deferred` whose result will be a list of socket objects
    which will later be handled by ``protocol_factory``.
        """
        res = start_serving(self, protocol_factory, host, port, ssl,
                            family, flags, sock, backlog, reuse_address)
        return maybe_async(res, event_loop=self, get_result=False)
    
    def create_datagram_endpoint(self, protocol_factory, local_addr=None,
                                 remote_addr=None, family=socket.AF_UNSPEC,
                                 proto=0, flags=0):
        res = create_datagram_endpoint(self, protocol_factory, local_addr,
                                       remote_addr, family, proto, flags)
        return maybe_async(res, event_loop=self, get_result=False)
        
        
    def stop_serving(self, sock):
        '''The argument should be a socket from the list returned by
:meth:`start_serving` method. The serving loop associated with that socket
will be stopped.'''
        self.remove_reader(sock.fileno())
        sock.close()
        
    def sock_connect(self, sock, address, timeout=None):
        '''Connect ``sock`` to the given ``address``.
        
        Returns a :class:`Deferred` whose result on success will be ``None``.'''
        res = sock_connect(self, sock, address)
        return maybe_async(res, event_loop=self, timeout=timeout)
    
    def sock_accept(self, sock, timeout=None):
        '''Accept a connection from a socket ``sock``.
        
        The socket must be in listening mode and bound to an address.
        Returns a :class:`Deferred` whose result on success will be a tuple
        ``(conn, peer)`` where ``conn`` is a connected non-blocking socket
        and ``peer`` is the peer address.'''
        timeout = timeout or DEFAULT_ACCEPT_TIMEOUT
        res = sock_accept(self, sock)
        return maybe_async(res, event_loop=self, timeout=timeout)
        
    #################################################    NON PEP METHODS
    def wake(self):
        '''Wake up the eventloop.'''
        if self.running and self._waker:
            self._waker.wake()
            
    def call_repeatedly(self, interval, callback, *args):
        """Call a ``callback`` every ``interval`` seconds. It handles
asynchronous results. If an error occur in the ``callback``, the chain is
broken and the ``callback`` won't be called anymore."""
        return LoopingCall(self, callback, args, interval)
    
    def call_every(self, callback, *args):
        '''Same as :meth:`call_repeatedly` with the only difference that
the ``callback`` is scheduled at every loop. Installing this callback cause
the event loop to poll with a 0 timeout all the times.'''
        return LoopingCall(self, callback, args)
    
    def call_now_threadsafe(self, callback, *args):
        '''Same as :meth:`call_soon_threadsafe` with the only exception that
if we are calling from the same thread of execution as this
:class:`EventLoop`, the ``callback`` is called immediately. Otherwise
:meth:`call_soon_threadsafe` is invoked.'''
        if self.tid != current_thread().ident:
            return self.call_soon_threadsafe(callback, *args)
        else:
            self._call(callback, *args)
            
    def has_callback(self, callback):
        if callback.deadline:
            return callback in self._scheduled
        else:
            return callback in self._callbacks

    #################################################    INTERNALS    
    def _before_run(self):
        ct = setid(self)
        self._name = ct.name
        if self._iothreadloop:
            set_event_loop(self)
    
    def _after_run(self):
        self.logger.info('Exiting %s', self)
        self._name = None
        self.tid = None
        
    def _raise_stop_event_loop(self, exc=None):
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
        timeout = timeout or self.poll_timeout
        self._num_loops += 1
        #
        # Compute the desired timeout
        if self._callbacks:
            timeout = 0
        elif self._scheduled:
            timeout = min(max(0, self._scheduled[0].deadline - self.timer()),
                          timeout)
        # poll events
        self._poll(timeout)
        #
        # append scheduled callback
        now = self.timer()
        while self._scheduled and self._scheduled[0].deadline <= now:
            self._callbacks.append(heappop(self._scheduled))
        #
        # Call the callbacks
        callbacks = self._callbacks
        todo = len(callbacks)
        call = self._call
        for i in range(todo):
            call(callbacks.popleft())
    
    def _poll(self, timeout):
        callbacks = self._callbacks
        io = self._io
        try:
            event_pairs = io.poll(timeout)
        except Exception as e:
            if self._raise_loop_error(e):
                raise
        except KeyboardInterrupt:
            raise StopEventLoop
        else:
            for fd, events in event_pairs:
                callbacks.append(partial(io.handle_events, self, fd, events))
        
    def _call(self, callback, *args):
        try:
            maybe_async(callback(*args), event_loop=self)
        except socket.error as e:
            if self._raise_loop_error(e):
                Failure(sys.exc_info()).log(
                    msg='Unhadled exception in event loop callback.')
        except Exception as e:
            Failure(sys.exc_info()).log(
                msg='Unhadled exception in event loop callback.')

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