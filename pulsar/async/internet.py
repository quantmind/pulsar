import io
import socket
from collections import deque

from pulsar.utils.internet import nice_address

from .access import logger

__all__ = ['BaseProtocol', 'Protocol', 'DatagramProtocol',
           'Transport', 'SocketTransport']

AF_INET6 = getattr(socket, 'AF_INET6', 0)

FAMILY_NAME = {socket.AF_INET: 'TCP'}
if AF_INET6:
    FAMILY_NAME[socket.AF_INET6] = 'TCP6'
if hasattr(socket, 'AF_UNIX'):
    FAMILY_NAME[socket.AF_UNIX] = 'UNIX'
    

class BaseProtocol:
    """ABC for base protocol class.

    Usually user implements protocols that derived from BaseProtocol
    like :class:`Protocol` or :class:`DatagramProtocol`.

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
    _transport = None

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

.. attribute:: event_loop

    The :class:`EventLoop` for this :class:`Transport`.
    
.. attribute:: protocol

    The :class:`Protocol` for this :class:`Transport`.
'''
    def get_extra_info(name, default=None):
        return None
        
    
class SocketTransport(Transport):
    '''A :class:`Transport` for sockets.
    
:parameter event_loop: Set the :attr:`Transport.event_loop` attribute.
:parameter sock: Set the :attr:`sock` attribute.
:parameter protocol: set the :class:`Transport.protocol` attribute.

When a new :class:`SocketTransport` is created, it adds a read handler
to the :attr:`Transport.event_loop` and notifies the :attr:`Transport.protocol`
that the connection is available via the :meth:`BaseProtocol.connection_made`
method.'''
    def __init__(self, event_loop, sock, protocol, extra=None,
                 max_buffer_size=None, read_chunk_size=None):
        self._protocol = protocol
        self._sock = sock
        self._sock.setblocking(False)
        self._sock_fd = sock.fileno()
        self._event_loop = event_loop
        self._closing = False
        self._extra = extra
        self._read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = []
        self._conn_lost = 0
        self._write_buffer = deque()
        self.logger = logger(event_loop)
        self._do_handshake()
    
    def __repr__(self):
        sock = self._sock
        if self._sock:
            address = sock.getsockname()
            family = FAMILY_NAME.get(sock.family, 'UNKNOWN')
            return '%s %s' % (family, nice_address(address))
        else:
            return '<closed>'
        
    def __str__(self):
        return self.__repr__()
    
    @property
    def sock(self):
        '''The socket for this :class:`SocketTransport`.'''
        return self._sock
     
    @property
    def writing(self):
        '''The :class:`SocketTransport` has data in the write buffer and it is
        not :attr:`closed`.'''
        return self._sock is not None and bool(self._write_buffer)
    
    @property
    def closing(self):
        '''The transport is about to close. In this state the transport is not
        listening for ``read`` events but it may still be writing, unless it
        is :attr:`closed`.'''
        return bool(self._closing)
    
    @property
    def closed(self):
        '''The transport is closed. No read/write operation available.'''
        return self._sock is None
    
    @property
    def protocol(self):
        return self._protocol
    
    @property
    def address(self):
        if self._sock:
            return self._sock.getsockname()
    
    @property
    def event_loop(self):
        return self._event_loop
    
    def fileno(self):
        if self._sock:
            return self._sock.fileno()
                
    def close(self, async=True, exc=None):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        :class:`BaseProtocol.connection_lost` method will (eventually) called
        with ``None`` as its argument.
        """
        if not self.closing:
            self._closing = True
            self._conn_lost += 1
            try:
                self._sock.shutdown(socket.SHUT_RD)
            except Exception:
                pass
            self._event_loop.remove_reader(self._sock_fd)
            if not async or not self.writing:
                self._event_loop.call_soon(self._shutdown, exc)
    
    def abort(self, exc=None):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The :class:`BaseProtocol.connection_lost` method will (eventually) be
        called with ``None`` as its argument.
        """
        self.close(async=False, exc=exc)
    
    def _do_handshake(self):
        pass
    
    def _read_ready(self):
        raise NotImplementedError
    
    def _check_closed(self):
        if self.closed:
            raise IOError("Transport is closed")
        elif self._closing:
            raise IOError("Transport is closing")
        
    def _shutdown(self, exc=None):
        if self._sock is not None:
            self._write_buffer = deque()
            self._event_loop.remove_writer(self._sock_fd)
            try:
                self._sock.shutdown(socket.SHUT_WR)
                self._sock.close()
            except Exception:
                pass
            self._sock = None
            self._protocol.connection_lost(exc)