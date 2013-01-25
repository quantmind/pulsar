from functools import partial

from pulsar import create_socket, ProtocolError
from pulsar.utils.sockets import create_socket, SOCKET_TYPES, wrap_socket
from pulsar.utils.pep import get_event_loop, set_event_loop, new_event_loop
from pulsar.utils.events import EventHandler

from .access import PulsarThread
from .defer import Deferred, coroutine
from .protocols import Connection, NOTHING
from .transports import Transport, LOGGER

__all__ = ['create_server', 'Server', 'Producer']
    

class ServerType(type):
    '''A simple metaclass for Servers.'''
    def __new__(cls, name, bases, attrs):
        new_class = super(ServerType, cls).__new__(cls, name, bases, attrs)
        type = getattr(new_class, 'TYPE', None)
        if type is not None:
            SOCKET_TYPES[type].server = new_class
        return new_class
            

class ServerConnection(Connection):
    
    def consume(self, data):
        while data:
            p = self.protocol
            response = self._current_consumer
            if response is None:
                self._processed += 1
                self._current_consumer = self._consumer_factory(self)
                self._producer.fire('pre_request', self._current_consumer)
                response = self._current_consumer 
            data = response.feed(data)
            if data and self._current_consumer:
                # if data is returned from the response feed method and the
                # response has not done yet raise a Protocol Error
                raise ProtocolError
    
    def finished(self, response, result=NOTHING):
        if response is self._current_consumer:
            self._producer.fire('post_request', self._current_consumer)
            self._current_consumer.on_finished.callback(result)
            self._current_consumer = None
        else:
            raise RuntimeError()

    
class Producer(object):
    '''A Producer of :class:`Connection` with remote servers or clients.
It is the base class for both :class:`Server` and :class:`ConnectionPool`.
The main method in this class is :meth:`new_connection` where a new
:class:`Connection` is created and added to the set of
:attr:`concurrent_connections`.

.. attribute:: concurrent_connections

    Number of concurrent active connections
    
.. attribute:: received

    Total number of received connections
    
.. attribute:: timeout

    number of seconds to keep alive an idle connection
    
.. attribute:: max_connections

    Maximum number of connections allowed. A value of 0 (default)
    means no limit.
'''
    connection_factory = None
    def __init__(self, max_connections=0, timeout=0, connection_factory=None):
        self._received = 0
        self._max_connections = max_connections
        self._concurrent_connections = set()
        self._timeout = timeout
        if connection_factory:
            self.connection_factory = connection_factory
    
    @property
    def timeout(self):
        return self._timeout
    
    @property
    def received(self):
        return self._received
    
    @property
    def max_connections(self):
        return self._max_connections
    
    @property
    def concurrent_connections(self):
        return len(self._concurrent_connections)
    
    def new_connection(self, protocol, consumer_factory, producer=None):
        ''''Called when a new connection is created'''
        if self._max_connections and self._received >= self._max_connections:
            raise RuntimeError('Too many connections')
        self._received = self._received + 1
        producer = producer or self 
        conn = self.connection_factory(protocol, producer, self._received,
                                       consumer_factory)
        self._concurrent_connections.add(conn)
        protocol.on_connection_lost.add_both(
                                partial(self._remove_connection, conn))
        return conn
    
    def close_connections(self, connection=None):
        '''Close *connection* if specified, otherwise close all
active connections.'''
        if connection:
            connection.transport.close()
        else:
            for connection in self._concurrent_connections:
                connection.transport.close()
            
    def _remove_connection(self, connection, *args):
        self._concurrent_connections.discard(connection)
        
    def close(self):
        raise NotImplementedError

     
class Server(ServerType('BaseServer', (Producer, EventHandler), {})):
    '''A :class:`Producer` for all server's listening for connections
on a socket. It is a producer of :class:`Transport` for server protocols.
    
.. attribute:: sock:

    The socket listening for client connections
    
.. attribute:: protocol_factory

    A factory producing the :class:`Protocol` for a socket created
    from a connection of a remote client with this server. This is a function
    or a :class:`Protocol` class which accept two arguments, the client address
    and the :attr:`consumer_factory` attribute. This attribute is used in
    the :meth:`create_connection` method. **By default pulsar uses a
    protocol appropiate to the socket type** .
    There shouldn't be any reason to change the default,
    it is here just in case.
    
.. attribute:: consumer_factory

    Callable or a :class:`ProtocolConsumer` class used
    to override the :class:`Protocol.consumer_factory` attribute. It produces
    :class:`ProtocolConsumer` which handle the receiving, decoding and
    sending of data.
    
.. attribute:: event_loop

    The :class:`EventLoop` running the server.
    
.. attribute:: address

    Server address, where clients send requests to.
    
.. attribute:: on_close

    A :class:`Deferred` called once the :class:`Server` is closed.
'''
    EVENTS = ('pre_request', 'post_request')
    connection_factory = ServerConnection
    protocol_factory = None
    
    def __init__(self, event_loop, sock, protocol_factory=None,
                 timeout=None, max_connections=0, consumer_factory=None,
                 connection_factory=None):
        super(Server, self).__init__(timeout=timeout,
                                     max_connections=max_connections,
                                     connection_factory=connection_factory)
        self._event_loop = event_loop
        self._sock = sock
        self.consumer_factory = consumer_factory
        self.on_close = Deferred()
        if protocol_factory:
            self.protocol_factory = protocol_factory
        self._event_loop.add_reader(self.fileno(), self.ready_read)
        LOGGER.debug('Registered server listening on %s', sock)
        
    def create_connection(self, sock, address):
        '''Create a new server :class:`Protocol` ready to serve its client.'''
        # Build the protocol
        sock = wrap_socket(self.TYPE, sock)
        protocol = self.protocol_factory(address)
        #Create the connection
        connection = self.new_connection(protocol, self.consumer_factory)
        transport = Transport(self._event_loop, sock, protocol,
                              timeout=self.timeout)
        connection.protocol.connection_made(transport)
    
    def __repr__(self):
        return str(self.address)
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def event_loop(self):
        return self._event_loop
    
    @property
    def address(self):
        return self._sock.address
    
    @property
    def sock(self):
        return self._sock
    
    @property
    def closed(self):
        return self._sock is None
    
    def fileno(self):
        if self._sock:
            return self._sock.fileno()
    
    def close(self):
        '''Close the server'''
        self._event_loop.remove_reader(self._sock.fileno())
        self.close_connections()
        self._sock.close()
        self._sock = None
        self.on_close.callback(self)
        
    def abort(self):
        self.close()
        
    def ready_read(self):
        '''Callback when a new connection is waiting to be served. This must
be implemented by subclasses.'''
        raise NotImplementedError

    @classmethod
    def create(cls, eventloop=None, sock=None, address=None, backlog=1024,
               name=None, close_event_loop=None, **kw):
        '''Create a new server!'''
        sock = create_socket(sock=sock, address=address, bindto=True,
                             backlog=backlog)
        server_type = SOCKET_TYPES[sock.TYPE].server
        eventloop = loop = eventloop or get_event_loop()
        server = None
        # The eventloop is cpubound
        if getattr(eventloop, 'cpubound', False):
            loop = get_event_loop()
            if loop is None:
                # No event loop available in the current thread.
                # Create one and set it as the event loop
                loop = new_event_loop()
                set_event_loop(loop)
                server = server_type(loop, sock, **kw)
                # Shutdown eventloop when server closes
                close_event_loop = True
                # start the server on a different thread
                eventloop.call_soon_threadsafe(_start_on_thread, name, server)
        server = server or server_type(loop, sock, **kw)
        if close_event_loop:
            server.on_close.add_both(lambda s: s.event_loop.stop())
        return server

create_server = Server.create

################################################################################
##    INTERNALS
def _start_on_thread(name, server):
    # we are on the actor request loop thread, therefore the event loop
    # should be already available if the tne actor is not CPU bound.
    PulsarThread(name=name, target=server.event_loop.run).start()
    
