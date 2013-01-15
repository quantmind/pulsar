from functools import partial

from pulsar import create_socket
from pulsar.utils.sockets import create_socket, SOCKET_TYPES, wrap_socket
from pulsar.utils.pep import get_event_loop, set_event_loop, new_event_loop

from .access import PulsarThread
from .defer import Deferred, coroutine
from .protocols import Connection, ProtocolError
from .transports import Transport, LOGGER

__all__ = ['create_server', 'ConcurrentServer', 'Server', 'Producer']


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
            

class SeverConnection(Connection):
    
    def consume(self, data):
        while data:
            p = self.protocol
            response = self._current_response
            if response is None:
                self._processed += 1
                self._current_response = self._response_factory(self)
                self._producer.fire('pre_request', self._current_response)
                response = self._current_response 
            data = response.feed(data)
            if data and self._current_response:
                # if data is returned from the response feed method and the
                # response has not done yet raise a Protocol Error
                raise ProtocolError
    
    def finished(self, response):
        if response is self._current_response:
            self._producer.fire('post_request', self._current_response)
            self._current_response = None
        else:
            raise RuntimeError()
    
class EventHandler(object):
    EVENTS = ('pre_request', 'post_request')
    
    def __new__(cls, *args, **kwargs):
        o = super(EventHandler, cls).__new__(cls)
        o.hooks = dict(((event, []) for event in cls.EVENTS))
        return o
        
    def bind_event(self, event, hook):
        '''Register an event hook'''
        self.hooks[event].append(hook)
        
    def fire(self, event, event_data):
        """Dispatches a hook dictionary on a given piece of data."""
        hooks = self.hooks
        if hooks and key in hooks:
            for hook in hooks[key]:
                try:
                    hook(event_data)
                except Exception:
                    LOGGER.exception('Unhandled error in %s hook', key)
    
    
class Producer(object):
    '''A Producer of connections with remote servers or clients. It is the base
class for both :class:`Server` and :class:`ConnectionPool`.

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
    connection_factory = SeverConnection
    def __init__(self, max_connections=0, timeout=0, connection_factory=None):
        self._received = 0
        self._max_connections = max_connections
        self._timeout = timeout
        self._concurrent_connections = set()
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
    
    def new_connection(self, protocol):
        '''Create a new connection using the :attr:`connection_factory`
attribute.'''
        self._received = self._received + 1
        conn = self.connection_factory(protocol, self, self._received)
        # wen connection is lost invoke _remove_connection
        protocol.on_connection_lost.add_both(self._remove_connection)
        self._concurrent_connections.add(c)
        if self.max_connections and self._received > self.max_connections:
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

     
class Server(ServerType('BaseServer', (Producer, EventHandler), {})):
    '''A :class:`Producer` for all server's listening for connections
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
    
.. attribute:: event_loop

    The :class:`EventLoop` running the server.
    
.. attribute:: address

    Server address, where clients send requests to.
    
.. attribute:: on_close

    A :class:`Deferred` called once the :class:`Server` is closed.
'''
    protocol_factory = None
    timeout = None
    
    def __init__(self, event_loop, sock, protocol_factory=None,
                  timeout=None, max_connections=0, response_factory=None,
                  connection_factory=None):
        super(Server, self).__init__(timeout=timeout,
                                     max_connections=max_connections,
                                     connection_factory=connection_factory)
        self._event_loop = event_loop
        self._sock = sock
        self.response_factory = response_factory
        self.on_close = Deferred()
        if protocol_factory:
            self.protocol_factory = protocol_factory
        self._event_loop.add_reader(self.fileno(), self.ready_read)
        
    def create_connection(self, sock, address):
        '''Create a new server :class:`Protocol` ready to serve its client.'''
        # Build the protocol
        sock = wrap_socket(self.TYPE, sock)
        protocol = self.protocol_factory(address)
        connection = self.new_connection(protocol)
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
    
