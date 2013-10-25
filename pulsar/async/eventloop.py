import os
import sys
import socket
from heapq import heappop
from functools import partial
from collections import deque
from threading import current_thread
from inspect import isgenerator
try:
    import signal
except ImportError:     # pragma    nocover
    signal = None

from pulsar.utils.system import close_on_exec
from pulsar.utils.pep import range
from pulsar.utils.internet import SOCKET_INTERRUPT_ERRORS
from pulsar.utils.exceptions import StopEventLoop, ImproperlyConfigured

from .access import asyncio, thread_local_data, LOGGER
from .defer import Task, Deferred, Failure, TimeoutError
from .stream import create_connection, start_serving, sock_connect, sock_accept
from .udp import create_datagram_endpoint
from .consts import DEFAULT_CONNECT_TIMEOUT, DEFAULT_ACCEPT_TIMEOUT
from .pollers import DefaultIO

__all__ = ['EventLoop', 'run_in_loop_thread']


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


def run_in_loop_thread(loop, callback, *args, **kwargs):
    '''Run ``callable`` in the ``loop`` thread.

    Return a :class:`Deferred`
    '''
    d = Deferred()

    def _():
        try:
            result = yield callback(*args, **kwargs)
        except Exception:
            result = sys.exc_info()
        d.set_result(result)
    loop.call_soon_threadsafe(_)
    return d


class EventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    '''Pulsar event loop policy'''
    def get_event_loop(self):
        return thread_local_data('_event_loop')

    def get_request_loop(self):
        return thread_local_data('_request_loop') or self.get_event_loop()

    def new_event_loop(self):
        return EventLoop()

    def set_event_loop(self, event_loop):
        """Set the event loop."""
        assert event_loop is None or isinstance(event_loop,
                                                asyncio.AbstractEventLoop)
        if getattr(event_loop, 'cpubound', False):
            thread_local_data('_request_loop', event_loop)
        else:
            thread_local_data('_event_loop', event_loop)


asyncio.set_event_loop_policy(EventLoopPolicy())

if not getattr(asyncio, 'fallback', False):
    from asyncio.base_events import BaseEventLoop
else:   # pragma    nocover
    BaseEventLoop = asyncio.BaseEventLoop

Handle = asyncio.Handle
TimerHandle = asyncio.TimerHandle


class LoopingCall(object):

    def __init__(self, loop, callback, args, interval=None):
        self._loop = loop
        self.callback = callback
        self.args = args
        self._cancelled = False
        interval = interval or 0
        if interval > 0:
            self.interval = interval
            self.handler = self._loop.call_later(interval, self)
        else:
            self.interval = None
            self.handler = self._loop.call_soon(self)

    @property
    def cancelled(self):
        return self._cancelled

    def cancel(self, result=None):
        '''Attempt to cancel the callback.'''
        self._cancelled = True

    def __call__(self):
        try:
            result = self._loop.async(self.callback(*self.args))
        except Exception:
            self.cancel()
            exc_info = sys.exc_info()
        else:
            result.add_callback(self._continue, self.cancel)
            return
        Failure(exc_info).log(msg='Exception in looping callback')

    def _continue(self, result):
        if not self._cancelled:
            handler = self.handler
            loop = self._loop
            if self.interval:
                handler._cancelled = False
                handler._when = loop.time() + self.interval
                loop._add_callback(handler)
            else:
                loop._ready.append(self.handler)


class EventLoop(BaseEventLoop):
    """A pluggable event loop which conforms with the pep-3156_ API.

    The event loop is the place where most asynchronous operations
    are carried out.

    .. attribute:: poll_timeout

        The timeout in seconds when polling with ``epolL``, ``kqueue``,
        ``select`` and so forth.

        Default: ``0.5``

    .. attribute:: tid

        The thread id where this event loop is running. If the
        event loop is not running this attribute is ``None``.

    """
    poll_timeout = 0.5
    tid = None
    pid = None
    exit_signal = None
    task_factory = Task

    def __init__(self):
        self.clear()

    def setup_loop(self, io=None, logger=None, poll_timeout=None,
                   iothreadloop=True):
        self._io = io or DefaultIO()
        self._signal_handlers = {}
        self.poll_timeout = poll_timeout if poll_timeout else self.poll_timeout
        self.logger = logger or LOGGER
        close_on_exec(self._io.fileno())
        self._iothreadloop = iothreadloop
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
        cpu = 'CPU bound ' if self.cpubound else ''
        return '%s%s %s' % (cpu, name, self.logger.name)

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
        return bool(self._ready or self._scheduled)

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
        if not self.is_running():
            self.call_soon(future.add_both, self._raise_stop_event_loop)
            handler = None
            if timeout:
                handler = self.call_later(timeout,
                                          self._raise_stop_event_loop)
            self.run()
            if handler:
                if future.done():
                    handler.cancel()
                else:
                    raise TimeoutError
            future.cancel()
            return future.result_or_throw()

    def stop(self):
        '''Stop the loop after the current event loop iteration is complete'''
        self.call_soon_threadsafe(self._raise_stop_event_loop)

    def is_running(self):
        '''``True`` if the loop is running.'''
        return bool(self._name)

    def run_in_executor(self, executor, callback, *args):
        '''Arrange to call ``callback(*args)`` in an ``executor``.

        Return a :class:`Deferred` called once the callback has finished.'''
        executor = executor or self._default_executor
        if executor is None:
            raise ImproperlyConfigured('No executor available')
        return executor.apply(callback, *args)

    #################################################    INTERNET NAME LOOKUPS
    def getaddrinfo(self, host, port, family=0, type=0, proto=0, flags=0):
        return socket.getaddrinfo(host, port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return socket.getnameinfo(sockaddr, flags)

    #################################################    I/O CALLBACKS
    def add_reader(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = Handle(callback, args)
        self._io.add_reader(file_descriptor(fd), handler)
        return handler

    def add_writer(self, fd, callback, *args):
        """Add a reader callback.  Return a Handler instance."""
        handler = Handle(callback, args)
        self._io.add_writer(file_descriptor(fd), handler)
        return handler

    def add_connector(self, fd, callback, *args):
        '''Add a connector callback. Return a Handler instance.'''
        handler = Handle(callback, args)
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
        '''Add a signal handler.

        Whenever signal ``sig`` is received, arrange for `callback(*args)` to
        be called. Returns an ``asyncio Handle`` which can be used to
        cancel the signal callback.
        '''
        self._check_signal(sig)
        handle = Handle(callback, args)
        self._signal_handlers[sig] = handle
        try:
            signal.signal(sig, self._handle_signal)
        except OSError as exc:
            del self._signal_handlers[sig]
            if not self._signal_handlers:
                try:
                    signal.set_wakeup_fd(-1)
                except ValueError as nexc:
                    self.logger.info('set_wakeup_fd(-1) failed: %s', nexc)
            if exc.errno == errno.EINVAL:
                raise RuntimeError('sig {} cannot be caught'.format(sig))
            else:
                raise

    def remove_signal_handler(self, sig):
        '''Remove the signal ``sig`` if it was installed and reinstal the
default signal handler ``signal.SIG_DFL``.'''
        self._check_signal(sig)
        try:
            del self._signal_handlers[sig]
        except KeyError:
            return False

        if sig == signal.SIGINT:
            handler = signal.default_int_handler
        else:
            handler = signal.SIG_DFL

        try:
            signal.signal(sig, handler)
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise RuntimeError('sig {} cannot be caught'.format(sig))
            else:
                raise

        if not self._signal_handlers:
            try:
                signal.set_wakeup_fd(-1)
            except ValueError as exc:
                logger.info('set_wakeup_fd(-1) failed: %s', exc)

        return True

    def _handle_signal(self, sig, frame):
        """Internal helper that is the actual signal handler."""
        handle = self._signal_handlers.get(sig)
        if handle is None:
            return  # Assume it's some race condition.
        if handle._cancelled:
            self.remove_signal_handler(sig)  # Remove it properly.
        else:
            handle._callback(*handle._args)

    #################################################    SOCKET METHODS
    def create_connection(self, protocol_factory, host=None, port=None,
                          ssl=None, family=0, proto=0, flags=0, sock=None,
                          local_addr=None, timeout=None):
        '''Creates a stream connection to a given internet host and port.

        It is the asynchronous equivalent of ``socket.create_connection``.

        :param protocol_factory: The callable to create the
            :class:`Protocol` which handle the connection.
        :param host: If host is an empty string or None all interfaces are
            assumed and a list of multiple sockets will be returned (most
            likely one for IPv4 and another one for IPv6)
        :param port:
        :param ssl:
        :param family:
        :param proto:
        :param flags:
        :param sock:
        :param local_addr: if supplied, it must be a 2-tuple
            ``(host, port)`` for the socket to bind to as its source address
            before connecting.
        :return: a :class:`Deferred` and its result on success is the
            ``(transport, protocol)`` pair.

        If a failure prevents the creation of a successful connection, an
        appropriate exception will be raised.
        '''
        timeout = timeout or DEFAULT_CONNECT_TIMEOUT
        res = create_connection(self, protocol_factory, host, port,
                                ssl, family, proto, flags, sock, local_addr)
        return self.async(res, timeout)

    def start_serving(self, protocol_factory, host=None, port=None, ssl=None,
                      family=socket.AF_UNSPEC, flags=socket.AI_PASSIVE,
                      sock=None, backlog=100, reuse_address=None):
        """Creates a TCP server bound to ``host`` and ``port``.

        :param protocol_factory: The :class:`Protocol` which handle server
            requests.
        :param host: If host is an empty string or None all interfaces are
            assumed and a list of multiple sockets will be returned (most
            likely one for IPv4 and another one for IPv6).
        :param port: integer indicating the port number.
        :param ssl: can be set to an SSLContext to enable SSL over
            the accepted connections.
        :param family: socket family can be set to either ``AF_INET`` or
            ``AF_INET6`` to force the socket to use IPv4 or IPv6.
            If not set it will be determined from host (defaults to
            ``AF_UNSPEC``).
        :param flags: is a bitmask for :meth:`getaddrinfo`.
        :param sock: can optionally be specified in order to use a
            pre-existing socket object.
        :param backlog: is the maximum number of queued connections
            passed to listen() (defaults to 100).
        :param reuse_address: tells the kernel to reuse a local socket in
            ``TIME_WAIT`` state, without waiting for its natural timeout to
            expire. If not specified will automatically be set to ``True``
            on UNIX.
        :return: a :class:`Deferred` whose result will be a list of socket
            objects which will later be handled by ``protocol_factory``.
        """
        res = start_serving(self, protocol_factory, host, port, ssl,
                            family, flags, sock, backlog, reuse_address)
        return self.async(res)

    def create_datagram_endpoint(self, protocol_factory, local_addr=None,
                                 remote_addr=None, family=socket.AF_UNSPEC,
                                 proto=0, flags=0):
        res = create_datagram_endpoint(self, protocol_factory, local_addr,
                                       remote_addr, family, proto, flags)
        return self.async(res)

    def stop_serving(self, sock):
        '''The argument should be a socket from the list returned by
:meth:`start_serving` method. The serving loop associated with that socket
will be stopped.'''
        self.remove_reader(sock.fileno())
        sock.close()

    def sock_connect(self, sock, address, timeout=None):
        '''Connect ``sock`` to the given ``address``.

        Returns a :class:`Deferred` whose result on success will be ``None``.
        '''
        return self.async(sock_connect(self, sock, address), timeout)

    def sock_accept(self, sock, timeout=None):
        '''Accept a connection from a socket ``sock``.

        The socket must be in listening mode and bound to an address.
        Returns a :class:`Deferred` whose result on success will be a tuple
        ``(conn, peer)`` where ``conn`` is a connected non-blocking socket
        and ``peer`` is the peer address.'''
        timeout = timeout or DEFAULT_ACCEPT_TIMEOUT
        return self.async(sock_accept(self, sock), timeout)

    #################################################    NON PEP METHODS
    def clear(self):
        self._ready = deque()
        self._scheduled = []

    def _write_to_self(self):
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

    def maybe_async(self, value):
        '''Run ``value`` in this event loop.

        If ``value`` is a :ref:`coroutine <coroutine>`, it is run immediately
        in this event loop.

        :return: either ``value`` or a :class:`Deferred`
        '''
        if isgenerator(value):
            return self.task_factory(value, loop=self)
        return value

    def async(self, value, timeout=None):
        '''Same as :meth:`maybe_asyc` but forcing a :class:`Deferred`
        return.
        '''
        value = self.maybe_async(value)
        if not isinstance(value, Deferred):
            d = Deferred()
            d.callback(value)
            return d
        elif timeout:
            value.set_timeout(timeout)
        return value

    #################################################    INTERNALS
    def _before_run(self):
        ct = setid(self)
        self._name = ct.name
        if self._iothreadloop:
            asyncio.set_event_loop(self)

    def _after_run(self):
        self.logger.info('Exiting %s', self)
        self._name = None
        self.tid = None

    def _raise_stop_event_loop(self, exc=None):
        if self.is_running():
            raise StopEventLoop

    def _check_signal(self, sig):
        """Internal helper to validate a signal.

        Raise ValueError if the signal number is invalid or uncatchable.
        Raise RuntimeError if there is a problem setting up the handler.
        """
        if signal is None:  # pragma    nocover
            raise RuntimeError('Signals are not supported')
        if not isinstance(sig, int):
            raise TypeError('sig must be an int, not {!r}'.format(sig))
        if not (1 <= sig < signal.NSIG):
            raise ValueError('sig {} out of range(1, {})'.format(sig,
                                                                 signal.NSIG))

    def _run_once(self, timeout=None):
        timeout = timeout or self.poll_timeout
        self._num_loops += 1
        #
        # Compute the desired timeout
        if self._ready:
            timeout = 0
        elif self._scheduled:
            timeout = min(max(0, self._scheduled[0]._when - self.time()),
                          timeout)
        # poll events
        self._poll(timeout)
        #
        # append scheduled callback
        now = self.time()
        while self._scheduled and self._scheduled[0]._when <= now:
            self._ready.append(heappop(self._scheduled))
        #
        # Run callbacks
        callbacks = self._ready
        todo = len(callbacks)
        for i in range(todo):
            exc_info = None
            handle = callbacks.popleft()
            try:
                if not handle._cancelled:
                    value = handle._callback(*handle._args)
                    if isgenerator(value):
                        self.task_factory(value, loop=self)
            except socket.error as e:
                if self._raise_loop_error(e):
                    exc_info = sys.exc_info()
            except Exception:
                exc_info = sys.exc_info()
            if exc_info:
                Failure(exc_info).log(
                    msg='Unhadled exception in event loop callback.')

    def _poll(self, timeout):
        callbacks = self._ready
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
                io.handle_events(self, fd, events)

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
