import socket
import logging

from collections import deque

from pulsar.utils.sockets import *
from pulsar.utils.httpurl import range
from pulsar.utils.structures import merge_prefix

LOGGER = logging.getLogger('pulsar.transports')


class Transport:
    '''Base class for pulsar transports'''
    def __init__(self, event_loop, sock, protocol, **params):
        self._sock = sock
        self._event_loop = event_loop
        self._protocol = protocol
        self._closing = False
        self.setup(**params)
        self._event_loop.add_reader(self._sock.fileno(), self.ready_read)
        self._event_loop.call_soon(self._protocol.connection_made, self)
    
    def setup(self, **params):
        pass
    
    @property
    def closed(self):
        return self._sock is None or self._closing
    
    @property
    def sock(self):
        return self._sock
    
    @property
    def event_loop(self):
        return self._event_loop
    
    def write(self, data):
        raise NotImplementedError
    
    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport."""
        for data in list_of_data:
            self.write(data)
    
    def ready_read(self):
        raise NotImplementedError
    
    def close(self):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        protocol's connection_lost() method will (eventually) called
        with None as its argument.
        """
        if not self.closed:
            self._closing = True
            self._event_loop.remove_reader(self._sock.fileno())
            self.on_close()
    
    def abort(self):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        self.close()
    
    def on_close(self):
        pass
    
    def _check_closed(self):
        if self.closing:
            raise IOError("Transport is closed")
        
    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()
        
        
class Client(Transport):
    '''Transport for a :class:`Transport` for a client socket, either
a Server client connection or a remote client.'''
    def setup(self, max_buffer_size=None, read_chunk_size=None, timeout=30):
        self.max_buffer_size = max_buffer_size or 104857600
        self.read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self.async_timeout = timeout
        self._write_buffer = deque()
        self._writing = False
        
    def write(self, data):
        """Write the given *data* to this stream. If there was previously
buffered write data and an old write callback, that callback is simply
overwritten with this new callback.

:rtype: a :class:`Deferred` instance or the number of bytes written.
        """
        self._check_closed()
        if data:
            assert isinstance(data, bytes)
            if len(data) > WRITE_BUFFER_MAX_SIZE:
                for i in range(0, len(data), WRITE_BUFFER_MAX_SIZE):
                    self._write_buffer.append(data[i:i+WRITE_BUFFER_MAX_SIZE])
            else:
                self._write_buffer.append(data)
        #
        if not self._writing:
            self._handle_write()

    def ready_read(self):
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # SSL sockets do some internal buffering, and if the data is
        # sitting in the SSL object's buffer select() and friends
        # can't see it; the only way to find out if it's there is to
        # try to read it.
        close = True
        while True:
            try:
                chunk = self._sock.recv(self.read_chunk_size)
            except socket.error as e:
                if se.args[0] == EWOULDBLOCK:
                    close = False
                else:
                    raise
            if not chunk:
                if close:
                    self.close()
                break
            self._protocol.data_received(chunk)
            
    def on_close(self):
        if not self._buffer:
            self._event_loop.call_soon(self._call_connection_lost, None)
            
    def _handle_write(self):
        # keep count how many bytes we write
        tot_bytes = 0
        while self._write_buffer:
            try:
                sent = self.sock.send(self._write_buffer[0])
                if sent == 0:
                    # With OpenSSL, after send returns EWOULDBLOCK,
                    # the very same string object must be used on the
                    # next call to send.  Therefore we suppress
                    # merging the write buffer after an EWOULDBLOCK.
                    # A cleaner solution would be to set
                    # SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, but this is
                    # not yet accessible from python
                    # (http://bugs.python.org/issue8240)
                    break
                merge_prefix(self._write_buffer, sent)
                self._write_buffer.popleft()
                tot_bytes += sent
            except socket.error as e:
                if async_error(e):
                    break
                else:
                    LOGGER.warning("Write error on %s: %s", self, e)
                    self.close()
                    return
        self._writing = bool(self._write_buffer)
        return tot_bytes
            
            
class ServerConnection(Client):
    
    def setup(self, server=None, session=None):
        self.server = server
        self.session = session
        self.server.concurrent_requests.add(self)
        
    def on_close(self):
        self.server.concurrent_requests.remove(self)
        

class ServerTransport(Transport):
    
    def ready_read(self):
        self.protocol.ready_read()

    def on_close(self):
        self.protocol.close()
        self._sock = None