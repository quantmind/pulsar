'''Replicate asyncio basic functionalities'''
from heapq import heappush

from pulsar.utils.pep import default_timer, ispy3k

fallback = True


if ispy3k:
    from concurrent.futures._base import Error, CancelledError, TimeoutError
else:   # pragma    nocover
    class Error(Exception):
        '''Raised when no other information is available on a Failure'''

    class CancelledError(Error):
        pass

    class TimeoutError(Error):
        pass


class InvalidStateError(Error):
    """The operation is not allowed in this state."""


class CoroutineReturn(BaseException):

    def __init__(self, value):
        self.value = value


def coroutine_return(value=None):
    raise CoroutineReturn(value)


class AbstractEventLoopPolicy(object):
    """Abstract policy for accessing the event loop."""

    def get_event_loop(self):
        """XXX"""
        raise NotImplementedError

    def set_event_loop(self, event_loop):
        """XXX"""
        raise NotImplementedError

    def new_event_loop(self):
        """XXX"""
        raise NotImplementedError


# Event loop policy.  The policy itself is always global, even if the
# policy's rules say that there is an event loop per thread (or other
# notion of context).  The default policy is installed by the first
# call to get_event_loop_policy().
_event_loop_policy = None


def get_event_loop_policy():
    """XXX"""
    global _event_loop_policy
    return _event_loop_policy


def set_event_loop_policy(policy):
    """XXX"""
    global _event_loop_policy
    assert policy is None or isinstance(policy, AbstractEventLoopPolicy)
    _event_loop_policy = policy


def get_event_loop():
    """XXX"""
    return get_event_loop_policy().get_event_loop()


def set_event_loop(event_loop):
    """XXX"""
    get_event_loop_policy().set_event_loop(event_loop)


def new_event_loop(**kwargs):
    """XXX"""
    return get_event_loop_policy().new_event_loop(**kwargs)


class AbstractEventLoop(object):
    '''This is just a signature'''

    def run_in_executor(self, executor, callback, *args):
        raise NotImplementedError


class Handle(object):
    _cancelled = False

    def __init__(self, callback, args):
        self._callback = callback
        self._args = args

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self._callback)
    __str__ = __repr__

    def cancel(self):
        '''Attempt to cancel the callback.'''
        self._cancelled = True


class TimerHandle(Handle):

    def __init__(self, when, callback, args):
        self._when = when
        self._callback = callback
        self._args = args

    def __lt__(self, other):
        return self._when < other._when


class BaseEventLoop(AbstractEventLoop):
    _default_executor = None

    def time(self):
        return default_timer()

    def call_later(self, delay, callback, *args):
        """Arrange for a callback to be called at a given time.

        Return a Handle: an opaque object with a cancel() method that
        can be used to cancel the call.

        The delay can be an int or float, expressed in seconds.  It is
        always a relative time.

        Each callback will be called exactly once.  If two callbacks
        are scheduled for exactly the same time, it undefined which
        will be called first.

        Any positional arguments after the callback will be passed to
        the callback when it is called.
        """
        return self.call_at(self.time() + delay, callback, *args)

    def call_at(self, when, callback, *args):
        """Like call_later(), but uses an absolute time."""
        timer = TimerHandle(when, callback, args)
        heappush(self._scheduled, timer)
        return timer

    def call_soon(self, callback, *args):
        """Arrange for a callback to be called as soon as possible.

        This operates as a FIFO queue, callbacks are called in the
        order in which they are registered.  Each callback will be
        called exactly once.

        Any positional arguments after the callback will be passed to
        the callback when it is called.
        """
        handle = TimerHandle(None, callback, args)
        self._ready.append(handle)
        return handle

    #################################################    THREAD INTERACTION
    def call_soon_threadsafe(self, callback, *args):
        """XXX"""
        handle = self.call_soon(callback, *args)
        self._write_to_self()
        return handle

    def set_default_executor(self, executor):
        self._default_executor = executor

    def _write_to_self(self):
        raise NotImplementedError

    def _add_callback(self, handle):
        """Add a Handle to ready or scheduled."""
        assert isinstance(handle, Handle), 'A Handle is required here'
        if handle._cancelled:
            return
        if isinstance(handle, TimerHandle):
            heappush(self._scheduled, handle)
        else:
            self._ready.append(handle)


_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'

class Future(object):
    _state = _PENDING
    _result = None
    _exception = None
    _loop = None
    _blocking = False  # proper use of future (yield vs yield from)
    _tb_logger = None

    def cancelled(self):
        '''pep-3156_ API method, it returns ``True`` if the :class:`Deferred`
was cancelled.'''
        return self._state == _CANCELLED

    def done(self):
        '''Returns ``True`` if the :class:`Deferred` is done.

        This is the case when it was called or cancelled.
        '''
        return self._state != _PENDING

    def result(self):
        """Return the result this future represents.

        If the future has been cancelled, raises CancelledError.  If the
        future's result isn't yet available, raises InvalidStateError.  If
        the future is done and has an exception set, this exception is raised.
        """
        if self._state == _CANCELLED:
            raise CancelledError
        if self._state != _FINISHED:
            raise InvalidStateError('Result is not ready.')
        if self._tb_logger is not None:
            self._tb_logger.clear()
            self._tb_logger = None
        if self._exception is not None:
            raise self._exception
        return self._result

    def __iter__(self):
        if not self.done():
            self._blocking = True
            yield self  # This tells Task to wait for completion.
        assert self.done(), "yield from wasn't used with future"
        coroutine_return(self.result())  # May raise too.


###########################################################################
##  ABSTRACT TRANSPORT

class BaseTransport(object):
    """Base ABC for transports."""

    def __init__(self, extra=None):
        if extra is None:
            extra = {}
        self._extra = extra

    def get_extra_info(self, name, default=None):
        """Get optional transport information."""
        return self._extra.get(name, default)

    def close(self):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        protocol's connection_lost() method will (eventually) called
        with None as its argument.
        """
        raise NotImplementedError


class ReadTransport(BaseTransport):
    """ABC for read-only transports."""

    def pause_reading(self):
        """Pause the receiving end.

        No data will be passed to the protocol's data_received()
        method until resume_reading() is called.
        """
        raise NotImplementedError

    def resume_reading(self):
        """Resume the receiving end.

        Data received will once again be passed to the protocol's
        data_received() method.
        """
        raise NotImplementedError


class WriteTransport(BaseTransport):
    """ABC for write-only transports."""

    def set_write_buffer_limits(self, high=None, low=None):
        """Set the high- and low-water limits for write flow control.

        These two values control when to call the protocol's
        pause_writing() and resume_writing() methods.  If specified,
        the low-water limit must be less than or equal to the
        high-water limit.  Neither value can be negative.

        The defaults are implementation-specific.  If only the
        high-water limit is given, the low-water limit defaults to a
        implementation-specific value less than or equal to the
        high-water limit.  Setting high to zero forces low to zero as
        well, and causes pause_writing() to be called whenever the
        buffer becomes non-empty.  Setting low to zero causes
        resume_writing() to be called only once the buffer is empty.
        Use of zero for either limit is generally sub-optimal as it
        reduces opportunities for doing I/O and computation
        concurrently.
        """
        raise NotImplementedError

    def get_write_buffer_size(self):
        """Return the current size of the write buffer."""
        raise NotImplementedError

    def write(self, data):
        """Write some data bytes to the transport.

        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        """
        raise NotImplementedError

    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport.

        The default implementation just calls write() for each item in
        the list/iterable.
        """
        for data in list_of_data:
            self.write(data)

    def write_eof(self):
        """Closes the write end after flushing buffered data.

        (This is like typing ^D into a UNIX program reading from stdin.)

        Data may still be received.
        """
        raise NotImplementedError

    def can_write_eof(self):
        """Return True if this protocol supports write_eof(), False if not."""
        raise NotImplementedError

    def abort(self):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        raise NotImplementedError


class Transport(ReadTransport, WriteTransport):
    """ABC representing a bidirectional transport.

    There may be several implementations, but typically, the user does
    not implement new transports; rather, the platform provides some
    useful transports that are implemented using the platform's best
    practices.

    The user never instantiates a transport directly; they call a
    utility function, passing it a protocol factory and other
    information necessary to create the transport and protocol.  (E.g.
    EventLoop.create_connection() or EventLoop.create_server().)

    The utility function will asynchronously create a transport and a
    protocol and hook them up by calling the protocol's
    connection_made() method, passing it the transport.

    The implementation here raises NotImplemented for every method
    except writelines(), which calls write() in a loop.
    """


class DatagramTransport(BaseTransport):
    """ABC for datagram (UDP) transports."""

    def sendto(self, data, addr=None):
        """Send data to the transport.

        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        addr is target socket address.
        If addr is None use target address pointed on transport creation.
        """
        raise NotImplementedError

    def abort(self):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        raise NotImplementedError


class BaseProtocol:
    """ABC for base protocol class.

    Usually user implements protocols that derived from BaseProtocol
    like Protocol or ProcessProtocol.

    The only case when BaseProtocol should be implemented directly is
    write-only transport like write pipe
    """

    def connection_made(self, transport):
        """Called when a connection is made.

        The argument is the transport representing the pipe connection.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """

    def pause_writing(self):
        """Called when the transport's buffer goes over the high-water mark.

        Pause and resume calls are paired -- pause_writing() is called
        once when the buffer goes strictly over the high-water mark
        (even if subsequent writes increases the buffer size even
        more), and eventually resume_writing() is called once when the
        buffer size reaches the low-water mark.

        Note that if the buffer size equals the high-water mark,
        pause_writing() is not called -- it must go strictly over.
        Conversely, resume_writing() is called when the buffer size is
        equal or lower than the low-water mark.  These end conditions
        are important to ensure that things go as expected when either
        mark is zero.

        NOTE: This is the only Protocol callback that is not called
        through EventLoop.call_soon() -- if it were, it would have no
        effect when it's most needed (when the app keeps writing
        without yielding until pause_writing() is called).
        """

    def resume_writing(self):
        """Called when the transport's buffer drains below the low-water mark.

        See pause_writing() for details.
        """


class Protocol(BaseProtocol):
    """ABC representing a protocol.

    The user should implement this interface.  They can inherit from
    this class but don't need to.  The implementations here do
    nothing (they don't raise exceptions).

    When the user wants to requests a transport, they pass a protocol
    factory to a utility function (e.g., EventLoop.create_connection()).

    When the connection is made successfully, connection_made() is
    called with a suitable transport object.  Then data_received()
    will be called 0 or more times with data (bytes) received from the
    transport; finally, connection_lost() will be called exactly once
    with either an exception object or None as an argument.

    State machine of calls:

      start -> CM [-> DR*] [-> ER?] -> CL -> end
    """

    def data_received(self, data):
        """Called when some data is received.

        The argument is a bytes object.
        """

    def eof_received(self):
        """Called when the other end calls write_eof() or equivalent.

        If this returns a false value (including None), the transport
        will close itself.  If it returns a true value, closing the
        transport is up to the protocol.
        """


class DatagramProtocol(BaseProtocol):
    """ABC representing a datagram protocol."""

    def datagram_received(self, data, addr):
        """Called when some datagram is received."""

    def connection_refused(self, exc):
        """Connection is refused."""
