import io
import socket
import logging
from inspect import isgenerator

from collections import deque

from pulsar.utils.sockets import *
from pulsar.utils.httpurl import range
from pulsar.utils.structures import merge_prefix

LOGGER = logging.getLogger('pulsar.transports')

WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb

__all__ = ['Transport', 'ClientTransport', 'StreamingClientTransport',
           'ServerConnectionTransport', 'ServerTransport']

    
class Transport:
    '''Base class for pulsar transports. Design to conform with pep-3156 as
close as possible until it is finalised.

.. attribute: protocol

    a the eventloop instance for this :class:`Transport`.
    
.. attribute: protocol

    a the protocol instance for this :class:`Transport`.
    
.. attribute: sock

    the socket for this :class:`Transport`.
'''
    def __init__(self, event_loop, sock, protocol, **params):
        self._sock = sock
        self._event_loop = event_loop
        self._protocol = protocol
        self._closing = False
        self.setup(**params)
        self._protocol.connection_made(self)
    
    def setup(self, **params):
        pass
    
    def __repr__(self):
        return self._protocol.__repr__()
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def protocol(self):
        return self._protocol
    
    @property
    def closed(self):
        return self._sock is None or self._closing
    
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
        
    ############################################################################
    ###    PEP-3156    METHODS
    def write(self, data):
        """Write some data bytes to the transport.
        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        """
        raise NotImplementedError
    
    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport.
If *list_of_data* is a generator, and during iteration an empty byte is yielded,
the function will postpone writing the remaining of the generator at the
next loop in the :attr:`eventloop`."""
        if isgenerator(list_of_data):
            self._write_lines_async(list_of_data)
        else:
            for data in list_of_data:
                self.write(data)
    
    def pause(self):
        """Pause the receiving end.

        No data will be passed to the protocol's data_received()
        method until resume() is called.
        """
        raise NotImplementedError

    def resume(self):
        """Resume the receiving end.

        Data received will once again be passed to the protocol's
        data_received() method.
        """
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError
    
    def abort(self):
        self.close()
    
    ############################################################################
    ###    INTERNALS
    def _write_lines_async(self, lines):
        try:
            result = next(lines)
            if result == b'':
                # stop writing and resume at next loop
                self._event_loop.call_soon(self._write_lines_async, lines)
            else:
                self.write(result)
                self._write_lines_async(lines)
        except StopIteration:
            pass
        
    def _ready_read(self):
        raise NotImplementedError
    
    def on_close(self):
        self._sock.close()
        self._sock = None
        
    def _check_closed(self):
        if self._sock is None:
            raise IOError("Transport is closed")
        
    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()
        
        
class BaseClientTransport(Transport):
    '''A :class:`Transport` for a client socket, either
a Server client connection or a remote client.'''
    default_timeout = 30
    largest_timeout = 604800
    def setup(self, max_buffer_size=None, read_chunk_size=None, timeout=None):
        timeout = timeout if timeout is not None else self.default_timeout
        self._timeout = max(0, timeout) or self.largest_timeout  
        self._max_buffer_size = max_buffer_size or 104857600
        self._read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = []
        self._write_buffer = deque()
        self._event_loop.add_writer(self.fileno(), self._ready_write)
        self._read_timeout = None
        self._writing = False
        self._paused = False
        
    ############################################################################
    ###    PEP-3156    METHODS
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
            self._ready_write()

    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False
        buffer = self._read_buffer
        self._read_buffer = []
        for data in buffer:
            self._data_received(chunk)
    
    def close(self):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        protocol's connection_lost() method will (eventually) called
        with None as its argument.
        """
        if not self.closed:
            self._closing = True
            self.close_read()
            if not self._write_buffer:
                self._event_loop.call_soon(self.on_close)
    
    def abort(self):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        if not self._closing:
            self._closing = True
            self.close_read()
            self._event_loop.remove_writer(self.fileno())
            self._write_buffer = deque()
        self.on_close()
    
    ############################################################################
    ###    PULSAR    METHODS
    def add_reader(self):
        self._event_loop.add_reader(self.fileno(), self._ready_read)
        self.add_read_timeout()
        
    def read_done(self):
        pass
    
    def on_close(self, exc=None):
        self._protocol.connection_lost(exc)
        self._sock = None
    
    def close_read(self):
        self._event_loop.remove_reader(self._sock.fileno())
        if self._read_timeout:
            self._read_timeout.cancel()
            self._read_timeout = None
    
    def _data_received(self, data):
        if self._paused:
            self._read_buffer.append(data)
        else:
            self._protocol.data_received(data)
            
    def _ready_read(self):
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # SSL sockets do some internal buffering, and if the data is
        # sitting in the SSL object's buffer select() and friends
        # can't see it; the only way to find out if it's there is to
        # try to read it.
        close = True
        while True:
            try:
                chunk = self._sock.recv(self._read_chunk_size)
            except socket.error as e:
                chunk = None
                if e.args[0] == EWOULDBLOCK:
                    close = False
                else:
                    self.close()
                    raise
            if not chunk:
                if close:
                    self.close()
                break
            if self._read_timeout:
                self._read_timeout.cancel()
                self._read_timeout = None
            try:
                self._data_received(chunk)
            except:
                self.abort()
                raise
        self.read_done()
            
    def _ready_write(self):
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
                if e.args[0] in (EWOULDBLOCK, ENOBUFS, EINPROGRESS):
                    break
                else:
                    LOGGER.warning("Write error on %s: %s", self, e)
                    self.abort()
                    return
        self._writing = bool(self._write_buffer)
        if not self._writing and self._closing:
            self._event_loop.call_soon(self.on_close)
        return tot_bytes
                
    def _timed_out(self, name, timeout):
        LOGGER.info('"%s" timed out on %s after %d seconds.'
                     % (name, self, timeout))
        self.on_timeout(name)
        
    def on_timeout(self, name):
        self.close()
        
    def add_read_timeout(self):
        if not self.closed and not self._read_timeout:
            self._read_timeout = self._event_loop.call_later(self._timeout,
                                                             self._timed_out,
                                                             'read',
                                                             self._timeout)


class StreamingClientTransport(BaseClientTransport):
    default_timeout = 30
    largest_timeout = 604800
    
    def setup(self, **kwargs):
        super(StreamingClientTransport, self).setup(**kwargs)
        self.add_reader()

    def read_done(self):
        self.add_read_timeout()


class ClientTransport(BaseClientTransport):
    ''':class:`ClientTransport` for non-streaming clients.'''
    default_timeout = 10
    largest_timeout = 30      
    
    
class ServerConnectionTransport(StreamingClientTransport):
    '''A :class:`StreamingClientTransport` for transport associated with client
sockets obtain from ``accept`` in TCP server sockets or ``recvfrom``
in UDP server sockets.'''
    def setup(self, server=None, session=None, **kwargs):
        self.server = server
        self.session = session
        self.server.concurrent_requests.add(self)
        super(ServerConnectionTransport, self).setup(**kwargs)
        
    def on_close(self):
        self.server.concurrent_requests.discard(self)
        super(ServerConnectionTransport, self).on_close()
        

class ServerTransport(Transport):
    connection_transport = ServerConnectionTransport
    
    def setup(self, **kwargs):
        self._event_loop.add_reader(self.fileno(), self._ready_read)
    
    def close(self):
        if not self.closed:
            self._closing = True
            self._event_loop.remove_reader(self._sock.fileno())
            self.on_close()
    
    def _ready_read(self):
        self._protocol.ready_read()

    def __call__(self, sock, protocol, session=None):
        # Build a transport for a client server connection
        sock = wrap_client_socket(sock)
        server = self._protocol
        return self.connection_transport(self.event_loop, sock, protocol,
                                         server=server, session=session,
                                         timeout=server.timeout)
        