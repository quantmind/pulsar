import io
import socket
from collections import deque

from pulsar.utils.internet import nice_address

from .access import asyncio, logger

__all__ = ['SocketTransport']


AF_INET6 = getattr(socket, 'AF_INET6', 0)
FAMILY_NAME = {socket.AF_INET: 'TCP'}
if AF_INET6:
    FAMILY_NAME[socket.AF_INET6] = 'TCP6'
if hasattr(socket, 'AF_UNIX'):
    FAMILY_NAME[socket.AF_UNIX] = 'UNIX'


class SocketTransport(asyncio.Transport):
    '''A :class:`Transport` for sockets.

    :parameter loop: Set the :attr:`Transport._loop` attribute.
    :parameter sock: Set the :attr:`sock` attribute.
    :parameter protocol: set the :class:`Transport.protocol` attribute.
    '''
    SocketError = socket.error

    def __init__(self, loop, sock, protocol, extra=None,
                 max_buffer_size=None, read_chunk_size=None):
        super(SocketTransport, self).__init__(extra)
        self._protocol = protocol
        self._sock = sock
        self._sock.setblocking(False)
        self._sock_fd = sock.fileno()
        self._loop = loop
        self._closing = False
        self._read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = []
        self._conn_lost = 0
        self._consecutive_writes = 0
        self._write_buffer = deque()
        self.logger = logger(loop)
        self._do_handshake()

    def __repr__(self):
        address = self.address
        if address:
            family = FAMILY_NAME.get(self._sock.family, 'UNKNOWN')
            return nice_address(address, family)
        else:
            return '<closed>'

    def __str__(self):
        return self.__repr__()

    @property
    def sock(self):
        '''The socket for this :class:`SocketTransport`.'''
        return self._sock

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
            try:
                return self._sock.getsockname()
            except (OSError, socket.error):
                return None

    def fileno(self):
        if self._sock:
            return self._sock.fileno()

    def get_extra_info(self, name, default=None):
        if name == 'socket':
            name = 'sock'
        return self.__dict__.get('_%s' % name, default)

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
            self._loop.remove_reader(self._sock_fd)
            if not async or not self._write_buffer:
                self._loop.call_soon(self._shutdown, exc)

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
        address = self.address
        if not address:
            raise IOError("Transport is closed")
        elif self._closing:
            raise IOError("Transport is closing")

    def _shutdown(self, exc=None):
        if self._sock is not None:
            self._write_buffer = deque()
            self._loop.remove_writer(self._sock_fd)
            try:
                self._sock.shutdown(socket.SHUT_WR)
                self._sock.close()
            except Exception:
                pass
            self._sock = None
            self._protocol.connection_lost(exc)
