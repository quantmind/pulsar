from functools import partial

from pulsar import create_socket
from pulsar.utils.sockets import create_socket, SOCKET_TYPES, wrap_socket
from pulsar.utils.pep import get_event_loop, set_event_loop, new_event_loop

from .access import PulsarThread
from .defer import Deferred
from .transports import Transport

__all__ = ['create_server', 'ConcurrentServer', 'Server']


class ConcurrentServer(object):
    
    def __new__(cls, *args, **kwargs):
        o = super(ConcurrentServer, cls).__new__(cls)
        o.received = 0
        o.concurrent_requests = set()
        return o
        
    @property
    def concurrent_request(self):
        return len(self.concurrent_requests)
    

class ServerType(type):
    '''A simple metaclass for Servers.'''
    def __new__(cls, name, bases, attrs):
        new_class = super(ServerType, cls).__new__(cls, name, bases, attrs)
        type = getattr(new_class, 'TYPE', None)
        if type is not None:
            SOCKET_TYPES[type].server = new_class
        return new_class
    

class Connection:
    
    def __init__(self, protocol, producer, session):
        self.protocol = protocol
        self.producer = producer
        self.session = session
        
    def __repr__(self):
        return '%s session %s' % (self.protocol, self.session)
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def transport(self):
        return self.protocol.transport
        
        
class Producer(object):
    connection_factory = Connection
    def __new__(cls, *args, **kwargs):
        o = super(Producer, cls).__new__(cls)
        o._received = 0
        o._concurrent_connections = set()
        return o
    
    @property
    def received(self):
        return self._received
    
    @property
    def concurrent_connections(self):
        return len(self._concurrent_connections)
    
    def new_connection(self, protocol, max_connections=0):
        self._received = self._received + 1
        c = self.connection_factory(protocol, self, self._received)
        protocol.on_connection_lost.add_both(self._remove_connection)
        self._concurrent_connections.add(c)
        if max_connections and self._received > max_connections:
            self.close()
        return c
    
    def close_connections(self, connection=None):
        if connection:
            connection.transport.close()
        else:
            for connection in self._concurrent_connections:
                connection.transport.close()
            
    def _remove_connection(self, *args):
        self._concurrent_connections.discard(self)
        
    def close(self):
        raise NotImplementedError
        
     
class Server(ServerType('BaseServer', (Producer,), {})):
    '''Base abstract class for all Server's listening for connections
on a socket. It is a producer of :class:`Transport` for server protocols.
    
.. attribute:: protocol_factory

    A factory producing the :class:`Protocol` for a socket created
    from a connection of a remote client with this server. This is a function
    or a :class:`Protocol` class which accept two arguments, the client address
    and the :attr:`response_factory` attribute. This attribute is used in
    the :meth:`create_connection` method.
    
.. attribute:: response_factory

    Optional callable or :class:`ProtocolResponse` class which can be used
    to override the :class:`Protocol.response_factory` attribute.
    
.. attribute:: timeout

    number of seconds to keep alive an idle connection
    
.. attribute:: event_loop

    The :class:`EventLoop` running the server.
    
.. attribute:: address

    Server address, where clients send requests to.
    
.. attribute:: on_close

    A :class:`Deferred` called once the :class:`Server` is closed.
    
.. attribute:: concurrent_connections

    Number of concurrent active connections
    
.. attribute:: received

    Total number of received connections
'''
    protocol_factory = None
    timeout = None
    
    def __init__(self, event_loop, sock, protocol_factory=None,
                  timeout=None, max_requests=0, response_factory=None):
        self._event_loop = event_loop
        self._sock = sock
        self.timeout = timeout if timeout is not None else self.timeout
        self.max_requests = max_requests
        self.response_factory = response_factory
        self.on_close = Deferred()
        if protocol_factory:
            self.protocol_factory = protocol_factory
        self._event_loop.add_reader(self.fileno(), self.ready_read)
        
    def create_connection(self, sock, address):
        '''Create a new server :class:`Protocol` ready to serve its client.'''
        # Build the protocol
        sock = wrap_socket(self.TYPE, sock)
        protocol = self.protocol_factory(address, self.response_factory)
        connection = self.new_connection(protocol, self.max_requests)
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
                loop = new_event_loop()
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
    event_loop = get_event_loop()
    if event_loop is None:
        set_event_loop(server.event_loop)
    PulsarThread(name=name, target=server.event_loop.run).start()
    
