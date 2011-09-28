import sys
import io
import logging
import socket
from collections import deque

iologger = logging.getLogger('pulsar.iostream')

MAX_BODY = 1024 * 128

__all__ = ['IOStream']


class IOStream(object):
    '''An utility class to write to and read from a blocking socket.
It is also used as base class for :class:`pulsar.AsyncIOStream` its
asyncronous equivalent.

:paramater callback: The streaming callback.
'''
    
    def __init__(self, socket = None, max_buffer_size=None,
                 read_chunk_size = None, family = None,
                 callback = None, actor = None, **kwargs):
        self.set_actor(actor)
        self.socket = self.get_socket(socket,family)
        self.socket.setblocking(self.blocking())
        self.max_buffer_size = max_buffer_size or 104857600
        self.read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = deque()
        self._write_buffer = deque()
        self._write_buffer_frozen = False
        self._read_callback = None
        self._write_callback = None
        self._close_callback = None
        self._connect_callback = None
        self._connecting = False
        self._state = None
        self.setup(kwargs)
    
    def fileno(self):
        '''Return the file descriptor of the socket.'''
        return self.socket.fileno()

    def set_actor(self, actor):
        '''Set the :class:`pulsar.Actor` instance handling the io stream.'''
        self.actor = actor
        if actor:
            self.log = actor.log or iologger
            self.ioloop = actor.ioloop
        else:
            self.log = iologger
            self.ioloop = None
        
    def blocking(self):
        '''Boolean indication if the socket is blocking.'''
        return True
    
    def wrap(self, callback):
        return callback
    
    def setup(self, kwargs):
        '''Called at sturtup. Override if you need to.'''
        pass
    
    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None
        
    def read(self, callback):
        """Reads all data from the socket until it is closed.
The callback will be called with chunks of data as they become available."""
        assert not self._read_callback, "Already reading"
        if self.closed():
            self._run_callback(callback, self._consume(self.read_buffer_size))
            return
        self._read_callback = self.wrap(callback)
        self.on_read()
        
    def write(self, data, callback=None):
        """Write the given data to this stream.

        If callback is given, we call it when all of the buffered write
        data has been successfully written to the stream. If there was
        previously buffered write data and an old write callback, that
        callback is simply overwritten with this new callback.
        """
        self._check_closed()
        self._write_buffer.append(data)
        self._write_callback = self.wrap(callback)
        self.on_write()
    
    #############################################    
    # CALLBACKS
    #############################################
    
    def on_read(self):
        self._handle_read()
        
    def on_write(self):
        self._handle_write()
    
    def _handle_read(self):
        '''Read from the socket until we get EWOULDBLOCK or equivalent.'''
        try:
            result = self.read_to_buffer()
        except Exception:
            self.close()
            return
        if result:
            self.read_from_buffer()
    
    def closed(self):
        '''Boolean indicating if the stream is closed.'''
        return self.socket is None
    
    def on_close(self):
        '''Closing callback'''
        pass
    
    def close(self):
        """Close this stream."""
        if self.socket is not None:
            self.on_close()
            self.socket.close()
            self.socket = None
            if self._close_callback:
                self._run_callback(self._close_callback)
    
    def connect(self, address, **kwargs):
        """Connects the socket to a remote address.

May only be called if the socket passed to the constructor was
not previously connected.  The address parameter is in the
same format as for socket.connect, i.e. a (host, port) tuple.
If callback is specified, it will be called when the
connection is completed.

Note that it is safe to call IOStream.write while the
connection is pending, in which case the data will be written
as soon as the connection is ready.  Calling IOStream read
methods before the socket is connected works on some platforms
but is non-portable.
        """
        self.socket.connect(address)
    
    def read_from_socket(self):
        """Attempts to read from the socket.
Returns the data read or None if there is nothing to read.
May be overridden in subclasses."""
        try:
            chunk = self.socket.recv(self.read_chunk_size)
        except socket.error as e:
            if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return None
            else:
                raise
        if not chunk:
            self.close()
            return None
        return chunk
    
    def read_to_buffer(self):
        """Reads from the socket and appends the result to the read buffer.
Returns the number of bytes read.

:rtype: the number of bytes read. Returns 0 if there is nothing
    to read (i.e. the read returns EWOULDBLOCK or equivalent).
    
On error closes the socket and raises an exception."""
        try:
            chunk = self.read_from_socket()
        except socket.error as e:
            # ssl.SSLError is a subclass of socket.error
            self.log.warning("Read error on %d: %s",
                             self.socket.fileno(), e)
            self.close()
            raise
        if chunk is None:
            return 0
        self._read_buffer.append(chunk)
        if self.read_buffer_size() >= self.max_buffer_size:
            self.log.error("Reached maximum read buffer size")
            self.close()
            raise IOError("Reached maximum read buffer size")
        return len(chunk)
    
    def read_from_buffer(self):
        """Attempts to complete the currently-pending read from the buffer.

        Returns True if the read was completed.
        """
        buffer = self._get_buffer(self._read_buffer)
        callback = self._read_callback
        self._read_callback = None
        self._read_bytes = None
        self._run_callback(callback, buffer)
    
    def read_buffer_size(self):
        '''Size of the reading buffer'''
        return sum(len(chunk) for chunk in self._read_buffer)
    
    def get_socket(self, fd, family = None):
        if hasattr(fd,'fileno'):
            return fd
        
        family = family or socket.AF_INET
        if fd is None:
            sock = socket.socket(family, socket.SOCK_STREAM)
        else:
            if hasattr(socket,'fromfd'):
                sock = socket.fromfd(fd, family, socket.SOCK_STREAM)
            else:
                raise ValueError('Cannot create socket from file deascriptor.\
 Not implemented in your system')
        if sock.family == socket.AF_INET:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return sock
    
    ######################################################################
    ##    INTERNALS
    ######################################################################
    
    def _run_callback(self, callback, *args, **kwargs):
        try:
            callback(*args, **kwargs)
        except:
            self.log.error("Uncaught exception, closing connection.",
                          exc_info=True)
            # Close the socket on an uncaught exception from a user callback
            # (It would eventually get closed when the socket object is
            # gc'd, but we don't want to rely on gc happening before we
            # run out of file descriptors)
            self.close()
            # Re-raise the exception so that IOLoop.handle_callback_exception
            # can see it and log the error
            raise
        
    def _handle_write(self):
        while self._write_buffer:
            try:
                if not self._write_buffer_frozen:
                    # On windows, socket.send blows up if given a
                    # write buffer that's too large, instead of just
                    # returning the number of bytes it was able to
                    # process.  Therefore we must not call socket.send
                    # with more than 128KB at a time.
                    buff = self._get_buffer(self._write_buffer, MAX_BODY)
                else:
                    buff = self._write_buffer.popleft() or b'' 
                num_bytes = self.socket.send(buff)
                self._write_buffer_frozen = False
            except socket.error as e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    # With OpenSSL, after send returns EWOULDBLOCK,
                    # the very same string object must be used on the
                    # next call to send.  Therefore we suppress
                    # merging the write buffer after an EWOULDBLOCK.
                    # A cleaner solution would be to set
                    # SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, but this is
                    # not yet accessible from python
                    # (http://bugs.python.org/issue8240)
                    self._write_buffer_frozen = True
                    break
                else:
                    self.log.warning("Write error on %d: %s",
                                    self.socket.fileno(), e)
                    self.close()
                    return
                
        if not self._write_buffer and self._write_callback:
            callback = self._write_callback
            self._write_callback = None
            self._run_callback(callback)

        
    def _check_closed(self):
        if not self.socket:
            raise IOError("Stream is closed")

    def _get_buffer(self, dq, size = None):
        if size is None:
            buff = b''.join(dq)
            dq.clear()
        else:
            remaining = size
            prefix = []
            while dq and remaining > 0:
                chunk = dq.popleft()
                if len(chunk) > remaining:
                    dq.appendleft(chunk[remaining:])
                    chunk = chunk[:remaining]
                prefix.append(chunk)
                remaining -= len(chunk)
                
            buff = b''.join(prefix)
        return buff
        