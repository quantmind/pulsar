import io
import socket
import logging
from inspect import isgenerator

from collections import deque

from pulsar.utils.sockets import *
from pulsar.utils.httpurl import range

LOGGER = logging.getLogger('pulsar.transports')

WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb

__all__ = ['Transport']


class Transport(object):
    '''Base class for pulsar transports. Design to conform with pep-3156_ as
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

.. attribute:: eventloop

    The :class:`EventLoop` for this :class:`Transport`.
    
.. attribute:: protocol

    The :class:`Protocol` for this :class:`Transport`.
    
.. attribute:: sock

    the socket/pipe for this :class:`Transport`.

.. attribute:: connecting

    ``True`` if the transport is connecting with remote server.
    
.. attribute:: writing

    ``True`` if the transport has data in its writing buffer.

.. attribute:: closing

    ``True`` if the transport is about to close.
        
.. attribute:: closed

    ``True`` if the transport is closed.
'''
    default_timeout = 30
    largest_timeout = 604800
    def __init__(self, event_loop, sock, protocol, max_buffer_size=None,
                  read_chunk_size=None, timeout=None):
        timeout = timeout if timeout is not None else self.default_timeout
        self._timeout = max(0, timeout)
        self._sock = sock
        self._event_loop = event_loop
        self._protocol = protocol
        self._closing = False
        self._paused = False
        self._read_timeout = None
        self._connecting = False
        self._read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = []
        self._write_buffer = deque()
        if self._event_loop:
            self._event_loop.add_writer(self.fileno(), self._ready_write)
            self.add_reader()
    
    def __repr__(self):
        return self._protocol.__repr__()
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def connecting(self):
        return self._connecting
    
    @property
    def writing(self):
        return bool(self._write_buffer)
    
    @property
    def closing(self):
        return bool(self._closing)
    
    @property
    def protocol(self):
        return self._protocol
    
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

    ############################################################################
    ###    PEP-3156    METHODS
    def write(self, data):
        '''Write some data bytes to the transport.
        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.'''
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
            self._ready_write()
    
    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport.
If *list_of_data* is a **generator**, and during iteration an empty byte is
yielded, the function will postpone writing the remaining of the generator
at the next loop in the :attr:`eventloop`."""
        if isgenerator(list_of_data):
            #self.pause()    #pause delivery of data until done with generator
            self._write_lines_async(list_of_data)
        else:
            for data in list_of_data:
                self.write(data)
    
    def pause(self):
        """A :class:`Transport` can be paused and resumed. Invoking this
method will cause the transport to buffer data coming from protocols but not
sending it to the :attr:`protocol`. In other words, no data will be passed to
the :meth:`Protocol.data_received` method until :meth:`resume` is called.
        """
        self._paused = True

    def resume(self):
        """Resume the receiving end. Data received will once again be
passed to the :meth:`Protocol.data_received` method."""
        self._paused = False
        buffer = self._read_buffer
        self._read_buffer = []
        for data in buffer:
            self._data_received(chunk)
    
    def close(self, async=True):
        """Closes the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        protocol's connection_lost() method will (eventually) called
        with None as its argument.
        """
        if not self.closing:
            self._closing = True
            self.close_read()
            if not async:
                self._write_buffer = deque()
                self._shutdown()
            elif not self.writing:
                self._event_loop.call_soon(self._shutdown)
    
    def abort(self):
        """Closes the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        self.close(async=False)
    
    ############################################################################
    ###    PULSAR TRANSPORT METHODS
    def connect(self):
        '''Connect this :class:`Transport` to a remote server and
returns ``self``.'''
        if not self.connecting:
            self._connecting = True
            try:
                if self._protocol.connect(self._sock):
                    self._connecting = False
                    self._protocol.connection_made(self)
            except Exception as e:
                #self._protocol.connection_made(self)
                self._protocol.connection_lost(e)
                raise
        return self
        
    def add_reader(self):
        self._event_loop.add_reader(self.fileno(), self._ready_read)
        self.add_read_timeout()
    
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
        
    def _check_closed(self):
        if self.closed:
            raise IOError("Transport is closed")
        elif self._closing:
            raise IOError("Transport is closing")
        
    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()
        
    def _shutdown(self, exc=None):
        if self._sock is not None:
            self._event_loop.remove_writer(self.fileno())
            self._sock.close()
            self._sock = None
            self._protocol.connection_lost(exc)
        
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
            except Exception:
                self.abort()
                raise
        self.add_read_timeout()
            
    def _ready_write(self):
        # keep count how many bytes we write
        if self.connecting:
            self._connecting = False
            self._protocol.connection_made(self)
        try:
            self._protocol.ready_write()
        except socket.error as e:
            LOGGER.warning("Write error on %s: %s", self, e)
            self.abort()
            return
        if self.writing:
            # more to do
            # TODO: should this be call_soon?
            self._event_loop.call_soon_threadsafe(self._ready_write)
        elif self._closing:
            # shutdown
            self._event_loop.call_soon(self._shutdown)
                
    def _timed_out(self):
        LOGGER.info('%s idle for %d seconds. Closing connection.',
                    self, self._timeout)
        self.close()
        
    def add_read_timeout(self):
        if not self.closed and not self._read_timeout and self._timeout:
            self._read_timeout = self._event_loop.call_later(self._timeout,
                                                             self._timed_out)    