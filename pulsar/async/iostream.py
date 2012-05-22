import sys
import io
import logging
import socket
import errno
from collections import deque
from threading import current_thread

from pulsar.utils.system import IObase
from pulsar import create_client_socket, wrap_socket

from .defer import Deferred, is_async, is_failure, thread_local_data
from .eventloop import deferred_timeout

iologger = logging.getLogger('pulsar.iostream')


__all__ = ['AsyncIOStream', 'thread_ioloop']


def thread_ioloop(ioloop=None):
    '''Returns the :class:`IOLoop` on the current thread if available.'''
    return thread_local_data('ioloop', ioloop)


class AsyncIOStream(IObase):
    '''An utility class to write to and read from a non-blocking socket.

It supports :meth:`write` and :meth:`read` operations with `callbacks` which
can be used to act when data has just been sent or has just been received.

This class was originally forked from tornado_ IOStream and subsequently
manipulated and adapted to pulsar :ref:`concurrent framework <design>`.

:attribute socket: socket which may either be connected or unconnected.
    If not supplied a new socket is created.
:parameter kwargs: dictionary of auxiliar parameters passed to the
    :meth:`on_init` callback.
'''
    _socket = None
    _state = None
    MAX_BODY = 1024 * 128
    def __init__(self, socket=None, max_buffer_size=None,
                 read_chunk_size=None, timeout = 5, **kwargs):
        self.socket = socket
        self.timeout = timeout
        self.max_buffer_size = max_buffer_size or 104857600
        self.read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = deque()
        self._write_buffer = deque()
        self._write_buffer_frozen = False
        self._read_callback = None
        self._read_length = None
        self._write_callback = None
        self._close_callback = None
        self._connect_callback = None
        self.log = iologger
    
    def __repr__(self):
        if self._socket:
            return '%s (%s)' % (self._socket, self.state_code)
        else:
            return '(closed)'
    
    def __str__(self):
        return self.__repr__()
    
    #######################################################    STATES
    @property
    def connecting(self):
        return self._connect_callback is not None
    
    @property
    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None

    @property
    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return bool(self._write_buffer)
    
    @property
    def closed(self):
        '''Boolean indicating if the stream is closed.'''
        return self.socket is None
    
    @property
    def state(self):
        return self._state
    
    @property
    def state_code(self):
        s = []
        if self.closed:
            return 'closed'
        if self.connecting:
            s.append('connecting')
        if self.writing:
            s.append('writing')
        if self.reading:
            s.append('reading')
        return ' '.join(s) if s else 'idle'
    
    @property
    def ioloop(self):
        return thread_ioloop()
    
    def fileno(self):
        '''Return the file descriptor of the socket.'''
        if self._socket:
            return self._socket.fileno()
    
    def getsockname(self):
        '''Return the socket's own address. This is useful to find out the
    port number of an IPv4/v6 socket, for instance. The format of the
    address returned depends on the address family.'''
        return self.socket.getsockname()
    
    def _set_socket(self, sock):
        if self._socket is None:
            self._socket = wrap_socket(sock)
            self._state = None
            if self._socket is not None:
                self._socket.setblocking(0)
        else:
            raise RuntimeError('Cannot set socket. Close the existing one.')
    def _get_socket(self):
        return self._socket
    socket = property(_get_socket, _set_socket)
    
    #######################################################    ACTIONS
    def connect(self, address):
        """Connects the socket to a remote address without blocking.
May only be called if the socket passed to the constructor was not available
or it was not previously connected.  The address parameter is in the
same format as for socket.connect, i.e. a (host, port) tuple or a string
for unix sockets.
If callback is specified, it will be called when the connection is completed.
Note that it is safe to call IOStream.write while the
connection is pending, in which case the data will be written
as soon as the connection is ready.  Calling IOStream read
methods before the socket is connected works on some platforms
but is non-portable."""
        if self._state is None and not self.connecting:
            if self.socket is None:
                self.socket = create_client_socket(address)
            try:
                self._socket.connect(address)
            except socket.error as e:
                # In non-blocking mode connect() always raises an exception
                if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                    raise
            d = Deferred(description = '%s connect callback' % self)
            self._connect_callback = d.callback
            return self._add_io_state(self.WRITE, d)
        else:
            if self._state is not None:
                raise RuntimeError('Cannot connect. State is %s.'\
                                   .format(self.state_code))
            else:
                raise RuntimeError('Cannot connect while connecting.')
        
    def read(self, length=None):
        """Reads data from the socket.
The callback will be called with chunks of data as they become available.
If no callback is provided, the callback of the returned deferred instance
will be used.

:parameter callback: Optional callback function with arity 1.
:rtype: a :class:`pulsar.Deferred` instance.

One common pattern of usage::

    def parse(data):
        ...
        
    io = AsyncIOStream(socket=sock)
    io.read().add_callback(parse)
    
"""
        assert not self.reading, "Already reading"
        d = Deferred(description='%s read callback' % self)
        if self.closed:
            data = self._get_buffer(self._read_buffer)
            if data:
                self._run_callback(d.callback, data)
                return d
            else:
                return
        self._read_callback = d.callback
        self._read_length = length
        return self._add_io_state(self.READ, d)
    
    def write(self, data):
        """Write the given data to this stream. If there was previously
buffered write data and an old write callback, that callback is simply
overwritten with this new callback.

:parameter callback: Optional callback function with arity 0.
:rtype: a :class:`Deferred` instance.
        """
        if data:
            d = Deferred(description='%s write callback' % self)
            try:
                self._check_closed()
            except Exception as e:
                d.callback(e)
                return d
            self._write_callback = d.callback
            self._write_buffer.append(data)
            self._handle_write()
            if self._write_buffer:
                return self._add_io_state(self.WRITE, d)
            return d
    
    def close(self):
        """Close this stream."""
        if self._socket is not None:
            self.log.debug('Closing {0}'.format(self))
            if self._state is not None:
                self.ioloop.remove_handler(self.fileno())
            self._socket.close()
            self._socket = None
            if self._close_callback:
                self._run_callback(self._close_callback)
                
    #######################################################    INTERNALS
    def read_to_buffer(self):
        """Reads from the socket and appends the result to the read buffer.
Returns the number of bytes read.

:rtype: the number of bytes read. Returns 0 if there is nothing
    to read (i.e. the read returns EWOULDBLOCK or equivalent).
    
On error closes the socket and raises an exception."""
        length = self._read_length or self.read_chunk_size
        # Read what is available in the buffer
        while True:
            try:
                chunk = self.socket.recv(length)
            except socket.error as e:
                self.log.warning("Read error on %d: %s",
                                 self.socket.fileno(), e)
                self.close()
                raise
            if chunk is None:
                break 
            self._read_buffer.append(chunk)
            if self.read_buffer_size() >= self.max_buffer_size:
                self.log.error("Reached maximum read buffer size")
                self.close()
                raise IOError("Reached maximum read buffer size")
            if len(chunk) < length:
                break
        return self.read_buffer_size()
    
    def read_buffer_size(self):
        '''Size of the reading buffer'''
        return sum(len(chunk) for chunk in self._read_buffer)
    
    def _run_callback(self, callback, *args, **kwargs):
        try:
            result = callback(*args, **kwargs)
            if is_failure(result):
                result.raise_all()
        except:
            # Close the socket on an uncaught exception from a user callback
            # (It would eventually get closed when the socket object is
            # gc'd, but we don't want to rely on gc happening before we
            # run out of file descriptors)
            self.close()
            # Re-raise the exception so that IOLoop.handle_callback_exception
            # can see it and log the error
            raise
    
    def _handle_connect(self):
        callback = self._connect_callback
        self._connect_callback = None
        self._run_callback(callback)
        
    def _handle_read(self):
        try:
            # Read from the socket until we get EWOULDBLOCK or equivalent.
            # SSL sockets do some internal buffering, and if the data is
            # sitting in the SSL object's buffer select() and friends
            # can't see it; the only way to find out if it's there is to
            # try to read it.
            result = self.read_to_buffer()
        except Exception:
            result = 0
        if result == 0:
            self.close()
        buffer = self._get_buffer(self._read_buffer)
            
        callback = self._read_callback
        if callback:
            self._read_callback = None
            self._read_bytes = None
            self._run_callback(callback, buffer)
            
    def _handle_write(self):
        tot_bytes = 0
        while self._write_buffer:
            try:
                if not self._write_buffer_frozen:
                    # On windows, socket.send blows up if given a
                    # write buffer that's too large, instead of just
                    # returning the number of bytes it was able to
                    # process.  Therefore we must not call socket.send
                    # with more than 128KB at a time.
                    buff = self._get_buffer(self._write_buffer, self.MAX_BODY)
                else:
                    buff = self._write_buffer.popleft() or b''
                num_bytes = len(buff)
                self.socket.sendall(buff)
                tot_bytes += num_bytes
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
            self._run_callback(callback,num_bytes)

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
        
    def set_close_callback(self, callback):
        """Call the given callback when the stream is closed."""
        self._close_callback = self.wrap(callback)

    def _handle_events(self, fd, events):
        # This is the actual callback from the event loop
        if not self.socket:
            self.log.warning("Got events for closed stream %d", fd)
            return
        try:
            if events & self.READ:
                self._handle_read()
            if not self.socket:
                return
            if events & self.WRITE:
                if self.connecting:
                    self._handle_connect()
                self._handle_write()
            if not self.socket:
                return
            if events & self.ERROR:
                # We may have queued up a user callback in _handle_read or
                # _handle_write, so don't close the IOStream until those
                # callbacks have had a chance to run.
                self.ioloop.add_callback(self.close)
                return
            state = self.ERROR
            if self.reading:
                state |= self.READ
            if self.writing:
                state |= self.WRITE
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.ioloop.update_handler(self.fileno(), self._state)
        except:
            self.close()
            raise

    def _add_io_state(self, state, deferred):
        if self.socket is None:
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            self._state = self.ERROR | state
            self.ioloop.add_handler(
                self, self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self.ioloop.update_handler(
                self.fileno(), self._state)
        return deferred_timeout(deferred, self.ioloop, timeout=self.timeout)

