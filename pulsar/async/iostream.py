import sys
import io
import logging
import socket
import errno
from collections import deque
from threading import current_thread

from .defer import Deferred, is_async

iologger = logging.getLogger('pulsar.iostream')


__all__ = ['IOStream','AsyncIOStream']


def make_callback(callback, description = None):
    if is_async(callback):
        return callback
    d = Deferred(description = description)
    return d.add_callback(callback, True)


class IOStream(object):
    '''An utility class to write to and read from a blocking socket.
It is also used as base class for :class:`AsyncIOStream` its
asynchronous equivalent counterpart.

It supports :meth:`write` and :meth:`read` operations with `callbacks` which
can be used to act when data has just been sent or has just been received.

This class was originally forked from tornado_ IOStream and subsequently
manipulated and adapted to pulsar :ref:`concurrent framework <design>`.

:parameter socket: Optional socket which may either be connected or unconnected.
    If not supplied a new socket is created.
:parameter kwargs: dictionary of auxiliar parameters passed to the
    :meth:`on_init` callback.
'''    
    def __init__(self, socket, max_buffer_size=None,
                 read_chunk_size = None, actor = None,
                 **kwargs):
        self.socket = socket
        self.socket.setblocking(self.blocking())
        self.MAX_BODY = 1024 * 128
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
        self._connecting = False
        self._state = None
        self.on_init(kwargs)
        self.set_actor(actor)
    
    def __repr__(self):
        fn = self.fileno() if self.socket else 'Closed'
        return '{0}'.format(fn)
    
    def __str__(self):
        fn = self.fileno() if self.socket else 'Closed'
        return '{0}({1})'.format(self.__class__.__name__,fn)
    
    def fileno(self):
        '''Return the file descriptor of the socket.'''
        return self.socket.fileno()
        
    def getsockname(self):
        '''Return the socket's own address. This is useful to find out the
    port number of an IPv4/v6 socket, for instance. The format of the
    address returned depends on the address family.'''
        return self.socket.getsockname()

    def set_actor(self, actor):
        '''Set the :class:`pulsar.Actor` instance handling the io stream.'''
        self.actor = actor
        if actor:
            self.log = actor.log or iologger
            self.ioloop = actor.ioloop
        else:
            self.log = iologger
            self.ioloop = getattr(current_thread(),'ioloop',None)
        
    def blocking(self):
        '''Boolean indication if the socket is blocking.'''
        return True
    
    def on_init(self, kwargs):
        '''Called at initialization. Override if you need to.'''
        pass
    
    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None
        
    def read(self, callback = None, length = None):
        """Reads data from the socket.
The callback will be called with chunks of data as they become available.
If no callback is provided, the callback of the returned deferred instance
will be used.

:parameter callback: Optional callback function with arity 1.
:rtype: a :class:`pulsar.Deferred` instance.

One common pattern of usage::

    def parse(data):
        ...
        
    io = IOStream(socket = sock)
    io.read().add_callback(parse)
    
"""
        assert not self._read_callback, "Already reading"
        d = make_callback(callback,
                          description = '{0} Read callback'.format(self))
        if self.closed():
            self._run_callback(d.callback, self._get_buffer())
            return
        self._read_callback = d.callback
        self._read_length = length
        self.on_read()
        return d
        
    def write(self, data, callback=None):
        """Write the given data to this stream. If callback is given,
we call it when all of the buffered write data has been successfully
written to the stream. If there was previously buffered write data and
an old write callback, that callback is simply overwritten with
this new callback.

:parameter callback: Optional callback function with arity 0.
:rtype: a :class:`pulsar.Deferred` instance.
        """
        d = make_callback(callback)
        try:
            self._check_closed()
        except:
            d.add_callback(sys.exc_info())
            return d
        self._write_callback = d.callback
        self._write_buffer.append(data)
        self.on_write()
        return d
    
    #############################################    
    # CALLBACKS
    #############################################
    
    def on_read(self):
        self._handle_read()
        
    def on_write(self):
        self._handle_write()
    
    def closed(self):
        '''Boolean indicating if the stream is closed.'''
        return self.socket is None
    
    def on_close(self):
        '''Closing callback'''
        pass
    
    def close(self):
        """Close this stream."""
        if self.socket is not None:
            self.log.debug('Closing {0}'.format(self))
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
    
    ######################################################################
    ##    INTERNALS
    ######################################################################
    
    def _run_callback(self, callback, *args, **kwargs):
        try:
            callback(*args, **kwargs)
        except:
            # Close the socket on an uncaught exception from a user callback
            # (It would eventually get closed when the socket object is
            # gc'd, but we don't want to rely on gc happening before we
            # run out of file descriptors)
            self.close()
            # Re-raise the exception so that IOLoop.handle_callback_exception
            # can see it and log the error
            raise
    
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
        
        
class AsyncIOStream(IOStream):
    """A specialized :class:`IOStream` class to write to and
read from a non-blocking socket.
    """
    def blocking(self):
        return False
    
    def on_read(self):
        self._add_io_state(self.ioloop.READ)
        
    def on_write(self):
        self._add_io_state(self.ioloop.WRITE)
        
    def on_close(self):
        if self._state is not None:
            self.ioloop.remove_handler(self.socket.fileno())

    def connect(self, address, callback=None):
        """Connects the socket to a remote address without blocking.

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
        self._connecting = True
        d = make_callback(callback,
                  description = '{0} Connect callback'.format(self))
        self._connect_callback = d.callback
        try:
            self.socket.connect(address)
        except socket.error as e:
            # In non-blocking mode connect() always raises an exception
            if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                raise
        self._add_io_state(self.ioloop.WRITE)
        return d

    def set_close_callback(self, callback):
        """Call the given callback when the stream is closed."""
        self._close_callback = self.wrap(callback)

    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None

    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return bool(self._write_buffer)

    def _handle_events(self, fd, events):
        # This is the actual callback from the event loop
        if not self.socket:
            self.log.warning("Got events for closed stream %d", fd)
            return
        try:
            if events & self.ioloop.READ:
                self._handle_read()
            if not self.socket:
                return
            if events & self.ioloop.WRITE:
                if self._connecting:
                    self._handle_connect()
                self._handle_write()
            if not self.socket:
                return
            if events & self.ioloop.ERROR:
                # We may have queued up a user callback in _handle_read or
                # _handle_write, so don't close the IOStream until those
                # callbacks have had a chance to run.
                self.ioloop.add_callback(self.close)
                return
            state = self.ioloop.ERROR
            if self.reading():
                state |= self.ioloop.READ
            if self.writing():
                state |= self.ioloop.WRITE
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.ioloop.update_handler(self.socket.fileno(), self._state)
        except:
            self.close()
            raise

    def _handle_connect(self):
        if self._connect_callback is not None:
            callback = self._connect_callback
            self._connect_callback = None
            self._run_callback(callback)
        self._connecting = False

    def _add_io_state(self, state):
        if self.socket is None:
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            self._state = self.ioloop.ERROR | state
            self.ioloop.add_handler(
                self, self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self.ioloop.update_handler(
                self.socket.fileno(), self._state)


