import io
import sys
import socket
import logging
from inspect import isgenerator
from collections import deque

from pulsar.utils import sockets
from pulsar.utils.pep import get_event_loop, range
from pulsar.utils.sockets import WRITE_BUFFER_MAX_SIZE, get_transport_type,\
                                 create_socket
from pulsar.async.defer import Deferred

LOGGER = logging.getLogger('pulsar.transport')


__all__ = ['Transport', 'SocketTransport', 'TransportProxy', 'create_transport']


def create_transport(protocol, sock=None, address=None, event_loop=None, 
                     source_address=None, **kw):
    '''Create a new connection with a remote server. It returns
a :class:`Transport` for the connection.'''
    sock = create_socket(sock=sock, address=address)
    sock.settimeout(0)
    if source_address:
        sock.bind(source_address)
    transport_factory = get_transport_type(sock.TYPE).transport
    event_loop = event_loop or get_event_loop()
    return transport_factory(event_loop, sock, protocol, **kw)


class TransportType(sockets.TransportType):
    '''A simple metaclass for Servers.'''
    def __new__(cls, name, bases, attrs):
        new_class = super(TransportType, cls).__new__(cls, name, bases, attrs)
        type = getattr(new_class, 'TYPE', None)
        if type is not None:
            cls.TRANSPORT_TYPES[type].transport = new_class
        return new_class


class Transport(TransportType('TransportBase', (), {})):
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
    def __init__(self, event_loop, sock, protocol, max_buffer_size=None,
                  read_chunk_size=None):
        self._protocol = protocol
        self._sock = sock
        self._event_loop = event_loop
        self._closing = False
        self._paused = False
        self._connector = None
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
    def connecting(self):
        return self._connector is not None
    
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
    
    ############################################################################
    ###    PULSAR TRANSPORT METHODS.
    def connect(self, address, timeout=None):
        '''Connect this :class:`Transport` to a remote server and
returns ``self``.'''
        self._connector = c = Connector(self, timeout=timeout)
        self.add_reader()
        self.add_connector()
        try:
            if self._protocol_connect(address):
                self.callback_connector(self)
        except Exception:
            self.callback_connector(sys.exc_info())
        return c
        
    def add_listener(self):
        '''Called by :class:`Server`'''
        self._event_loop.add_reader(self.fileno(), self._protocol_accept)
        
    def add_reader(self):
        '''Add reader to the event loop. An optional *error_handler*
can be passed. The error handler will be called once an exception occurs
during asynchronous reading, with the exception instance
as only attribute.'''
        self._event_loop.add_reader(self.fileno(), self._ready_read)
        
    def add_writer(self):
        '''Add writer to the event loop'''
        self._event_loop.add_writer(self.fileno(), self._ready_write)
        
    def add_connector(self):
        '''Add writer to the event loop'''
        self._event_loop.add_connector(self.fileno(), self.callback_connector)
    
    def remove_writer(self):
        self._event_loop.remove_writer(self.fileno())
        
    def callback_connector(self, result=None):
        c = self._connector
        if c:
            self._connector = None
            self._event_loop.remove_connector(self.fileno())
            return c.callback(result)
        
    def is_stale(self):
        if self._sock:
            return sockets.is_closed(self._sock)
        else:
            return True
        
    ############################################################################
    ###    INTERNALS
    def _data_received(self, data):
        if self._paused:
            self._read_buffer.append(data)
        else:
            self._protocol.data_received(data)
                 
    def _check_closed(self):
        if self.closed:
            raise IOError("Transport is closed")
        elif self._closing:
            raise IOError("Transport is closing")
        
    def _shutdown(self, exc=None):
        if self._sock is not None:
            self._write_buffer = deque()
            self._event_loop.remove_writer(self.fileno())
            self._sock.close()
            self._sock = None
            if self.connecting:
                if not exc:
                    try:
                        raise RuntimeError('Could not connect. Closing.')
                    except RuntimeError:
                        exc = sys.exc_info()
                self._connector.callback(exc)
            else:
                self._protocol.connection_lost(exc)
        
    def _ready_read(self):
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # If any other error occur, abort the connection and re-raise.
        passes = 0
        chunk = True
        while chunk:
            try:
                chunk = self._protocol_read()
                if chunk:
                    self._data_received(chunk)
                elif not passes and chunk == b'':
                    # We got empty data. Close the socket
                    self.close()
                passes += 1
            except Exception:
                self.abort(exc=sys.exc_info())
                raise
                
    def _ready_write(self):
        if self.writing:
            try:
                self._protocol_write()
            except socket.error as e:
                # error during a write, abort
                self.abort(exc=e)
                raise
        if not self.writing:
            self.remove_writer()
            if self._closing:
                self._event_loop.call_soon(self._shutdown)
        
    def _do_write(self):
        if self.writing:
            try:
                self._protocol_write()
            except socket.error:
                self.abort(exc=sys.exc_info())
                raise
        if self.writing:
            self.add_writer()
        elif self._closing:
            self._event_loop.call_soon(self._shutdown)
    
    ############################################################################
    ##    PURE VIRTUALS
    def _protocol_connect(self):
        raise NotImplementedError
    
    def _protocol_read(self):
        raise NotImplementedError
    
    def _protocol_write(self):
        raise NotImplementedError
    
    def _protocol_accept(self):
        raise NotImplementedError
            

class TransportProxy(object):
    '''Provides :class:`Transport` like methods and attributes.'''
    _transport = None
    
    def __repr__(self):
        if self._transport:
            return self._transport.__repr__()
        else:
            return '<closed>'
        
    def __str__(self):
        return self.__repr__()
    
    @property
    def transport(self):
        return self._transport
    
    @property
    def event_loop(self):
        if self._transport:
            return self._transport.event_loop
    
    @property
    def sock(self):
        if self._transport:
            return self._transport.sock
        
    @property
    def closing(self):
        return self._transport.closing if self._transport else True
    
    @property
    def closed(self):
        return self._transport.closed if self._transport else True
    
    def fileno(self):
        if self._transport:
            return self._transport.fileno()
    
    def is_stale(self):
        return self._transport.is_stale() if self._transport else True
    
    def close(self, async=True, exc=None):
        if self._transport:
            self._transport.close(async=async, exc=exc)
        
    def abort(self, exc=None):
        self.close(async=False, exc=exc)
        
        
class Connector(Deferred, TransportProxy):
    '''A :class:`Connector` is in place of the :class:`Connection` during
a connection operation. Once the connection with end-point is established, the
connector fires its callbacks.'''
    processed = 0
    def __init__(self, transport, timeout=None):
        self._transport = transport
        self._consumer = None
        super(Connector, self).__init__(timeout=timeout or 10,
                                        event_loop=transport.event_loop)
        self.add_callback(self._connection_made, self._connection_failure)
    
    def __repr__(self):
        return 'Connector for %s' % self._transport
        
    def _connection_made(self, result):
        self._transport._protocol.connection_made(self._transport)
        return self._transport._protocol
        
    def _connection_failure(self, failure):
        self._transport._protocol.connection_lost(failure)
        return failure
        
    def set_consumer(self, consumer):
        '''Set the :class:`ProtocolConsumer` which will consume the protocol
once the connection is made.'''
        consumer._connection = self
        # add callback and errorback to self
        self.add_callback(lambda c: c.set_consumer(consumer),
                          lambda f: consumer.connection_lost(f))
        
    def finished(self, consumer, exc=None):
        # Called by the protocol consumer when the connector is still in place
        # of the connection
        consumer.fire_event('finish', exc)
        # important, return the original result
        return exc
        