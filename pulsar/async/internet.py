import io
from collections import deque

from pulsar.utils.internet import WRITE_BUFFER_MAX_SIZE
from . import tcp
from .defer import maybe_async


class BaseProtocol:
    """ABC for base protocol class.

    Usually user implements protocols that derived from BaseProtocol
    like Protocol or ProcessProtocol.

    The only case when BaseProtocol should be implemented directly is
    write-only transport like write pipe
    """

    def connection_made(self, transport):
        """Called when a connection is made.

        The argument is the :class:`Transport` representing the pipe connection.
        When the connection is closed, :meth:`connection_lost` is called.
        """

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """


class Protocol(BaseProtocol):
    """ABC representing a protocol for a stream.

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

        The default implementation does nothing.

        TODO: By default close the transport.  But we don't have the
        transport as an instance variable (connection_made() may not
        set it).
        """


class DatagramProtocol(BaseProtocol):
    """ABC representing a datagram protocol."""

    def datagram_received(self, data, addr):
        """Called when some datagram is received."""

    def connection_refused(self, exc):
        """Connection is refused."""
    
    
class Transport(object):
    '''Base class for transports. Design to conform with pep-3156_ as
close as possible until it is finalised. A transport is an abstraction on top
of a socket or something similar.
Form pep-3153_:

Transports talk to two things: the other side of the
connection on one hand, and a :attr:`protocol` on the other. It's a bridge
between the specific underlying transfer mechanism and the protocol.
Its job can be described as allowing the protocol to just send and
receive bytes, taking care of all of the magic that needs to happen to those
bytes to be eventually sent across the wire.

The primary feature of a transport is sending bytes to a protocol and receiving
bytes from the underlying protocol. Writing to the transport is done using
the :meth:`write` and :meth:`writelines` methods. The latter method is a
performance optimisation, to allow software to take advantage of specific
capabilities in some transport mechanisms.

.. attribute:: event_loop

    The :class:`EventLoop` for this :class:`Transport`.
    
.. attribute:: protocol

    The :class:`Protocol` for this :class:`Transport`.
    
.. attribute:: sock

    the socket/pipe for this :class:`Transport`.

.. attribute:: connecting

    ``True`` if the transport is connecting with an end-point.
    
.. attribute:: writing

    The transport has data in the write buffer and it is not :attr:`closed`.

.. attribute:: closing

    The transport is about to close. In this state the transport is not
    listening for ``read`` events but it may still be writing, unless it
    is :attr:`closed`.
        
.. attribute:: closed

    The transport is closed. No read/write operation avaibale.
'''
    closed = False
    def write(self, data):
        '''Write some data bytes to the transport.
        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.'''
        raise NotImplementedMethod
    
    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport.
If *list_of_data* is a **generator**, and during iteration an empty byte is
yielded, the function will postpone writing the remaining of the generator
at the next loop in the :attr:`eventloop`."""
        for data in list_of_data:
            self.write(data)
    
    def pause(self):
        """A :class:`Transport` can be paused and resumed. Invoking this
method will cause the transport to buffer data coming from protocols but not
sending it to the :attr:`protocol`. In other words, no data will be passed to
the :meth:`Protocol.data_received` method until :meth:`resume` is called.
        """
        raise NotImplementedError

    def resume(self):
        """Resume the receiving end. Data received will once again be
passed to the :meth:`Protocol.data_received` method."""
        raise NotImplementedError
    
    def close(self, async=True, exc=None):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        protocol's connection_lost() method will (eventually) called
        with None as its argument.
        """
        raise NotImplementedError
    
    def abort(self, exc=None):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        self.close(async=False, exc=exc)
        
    
class SocketTransport(Transport):
    '''A class:`Transport` based on sockets. It handles events implemented by
the :class:`EventHandler`.
    
.. attribute:: connecting

    The transport is connecting, all writes are buffered until connection
    is established.
    
.. attribute:: writing

    The transport has data in the write buffer and it is not :class:`closed`.
    
**One Time Events**

* **connection_made** fired when a connections made (for client connections)
* **closing** when transport is about to close
* **connection_lost** fired when a the connections with end-point is lost

**Many Times Events**

* **data_received** fired when new data has arrived
'''
    def __init__(self, event_loop, sock, protocol, extra=None,
                 max_buffer_size=None, read_chunk_size=None):
        self._protocol = protocol
        self._sock = sock
        self._sock_fd = sock.fileno()
        self._event_loop = event_loop
        self._closing = False
        self._paused = False
        self._extra = extra
        self._read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = []
        self._write_buffer = deque()
    
    def __repr__(self):
        try:
            return self._sock.__repr__()
        except Exception:
            return '<closed>'
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def writing(self):
        return self._sock is not None and bool(self._write_buffer)
    
    @property
    def closing(self):
        return bool(self._closing)
    
    @property
    def closed(self):
        return self._sock is None
    
    @property
    def sock(self):
        return self._sock
    
    @property
    def address(self):
        if self._sock:
            return self._sock.address
    
    @property
    def event_loop(self):
        return self._event_loop
    
    def fileno(self):
        if self._sock:
            return self._sock.fileno()

    def write(self, data):
        self._check_closed()
        writing = self.writing
        if data:
            assert isinstance(data, bytes)
            if len(data) > WRITE_BUFFER_MAX_SIZE:
                for i in range(0, len(data), WRITE_BUFFER_MAX_SIZE):
                    self._write_buffer.append(data[i:i+WRITE_BUFFER_MAX_SIZE])
            else:
                self._write_buffer.append(data)
        # Try to write only when not waiting for write callbacks
        if not self.connecting and not writing:
            self._do_write()
    
    def pause(self):
        if not self._paused:
            self._paused = True

    def resume(self):
        if self._paused:
            self._paused = False
            buffer = self._read_buffer
            self._read_buffer = []
            for data in buffer:
                self._data_received(chunk)
    
    def close(self, async=True, exc=None):
        if not self.closing:
            self._closing = True
            self._event_loop.remove_reader(self._sock.fileno())
            if async and not self.writing:
                self._event_loop.call_soon(self._shutdown, exc)
        if not async:
            self._shutdown(exc)
    