from pulsar.utils.pep import get_event_loop
from pulsar.utils.sockets import SOCKET_TYPES, create_socket

from .defer import Deferred, pass_through
from .protocols import ProtocolConsumer, Connection, NOTHING
from .transports import Transport
from .servers import Producer, EventHandler

__all__ = ['create_connection', 'ConnectionPool', 'Client',
           'ClientProtocolConsumer']

transports= set()

def create_connection(address, timeout=0, source_address=None):
    '''Create a connection with a remote server'''
    sock = create_socket(address=address, bindto=False)
    sock.settimeout(timeout)
    if source_address:
        sock.bind(source_address)
    protocol_factory = SOCKET_TYPES[sock.TYPE].server.protocol_factory
    protocol = protocol_factory(address)
    event_loop = get_event_loop() if timeout == 0 else None
    transport = Transport(event_loop, sock, protocol)
    transports.add(transport)
    return transport.connect().protocol


class ClientEventHandler(EventHandler):
    EVENTS = ('pre_request', 'post_request', 'response')
    
    
class Request(ClientEventHandler):
    '''A :class:`Client` request class.'''
    def __init__(self, address, timeout=0):
        self.address = address
        self.timeout = timeout
    

class ClientProtocolConsumer(ProtocolConsumer):
    '''A :class:`ProtocolConsumer` for a :class:`Client`.
    
.. attribute:: request

    The :class:`Request` instance.

.. attribute:: consumer

    Optional callable which can be used by the feed method to send data to
    another listening consumer. This can be used to stream data as
    it arrives (for example).
'''
    def __init__(self, connection, request, consumer):
        super(ClientProtocolConsumer, self).__init__(connection)
        connection.set_response(self)
        self.request = request
        self.consumer = consumer or pass_through
        self.when_ready = Deferred()
        
    def begin(self):
        '''Connect, send data and wait for results.'''
        self.protocol.on_connection.add_callback(self.send, self.finished)
        return self
    
    def feed(self, data):
        '''Called when new data has arrived from the remote server.'''
        raise NotImplementedError
            
    def send(self, *args):
        '''Write the request to the server. Must be implemented by
subclasses. This method is invoked by the :meth:`begin` method
once the connection is established. **Not called directly**.'''
        raise NotImplementedError
        
        
class ClientConnection(Connection):
    
    def set_response(self, response):
        assert self._current_response is None, 'Response is not None'
        self._current_response = response
        self._processed += 1
        
    def consume(self, data):
        while data:
            p = self.protocol
            response = self._current_response
            if response is None:
                raise ProtocolError 
            data = response.feed(data)
            if data and self._current_response:
                # if data is returned from the response feed method and the
                # response has not done yet raise a Protocol Error
                raise ProtocolError
    
    def finished(self, response, result=NOTHING):
        if response is self._current_response:
            self._producer.fire('response', self._current_response)
            result = self if result is NOTHING else result
            self._current_response.when_ready.callback(result)
            self._current_response = None
            response._connection = None
        else:
            raise RuntimeError()
    
        
class ConnectionPool(Producer):
    '''A :class:`Producer` of of active connections for client
protocols. It maintains a live set of connections.

.. attribute:: address

    Address to connect to
    '''    
    def __init__(self, request, **params):
        super(ConnectionPool, self).__init__(**params)
        self._address = request.address
        self._available_connections = []
        
    @classmethod
    def get(cls, request, pools, **params):
        '''Build a new :class:`ConnectionPool` if not already available in
*pools* dictionary.'''
        self = cls(request, **params)
        if self not in pools:
            pools[self] = self
        return pools[self]
    
    def __hash__(self):
        return hash((self.address, self.timeout))
    
    @property
    def address(self):
        return self._address
    
    def release(self, connection):
        "Releases the connection back to the pool"
        self._concurrent_connections.remove(connection)
        self._available_connections.append(connection)

    def remove(self, connection):
        '''Remove the *connection* from the pool'''
        self._in_use_connections.remove(connection)
        try:
            connection.close()
        except:
            pass
        
    def get_or_create_connection(self, client):
        "Get or create a new connection for *client*"
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = None
        else:
            # we have a connection, lets added it to the concurrent set
            self._concurrent_connections.add(connection)
        if connection is None:
            # build protocol and build the new connection
            protocol = self.build_protocol(client)
            connection = self.new_connection(protocol, client.response_factory,
                                             client)
        return connection
    
    def build_protocol(self, client):
        return create_connection(self._address, timeout=self._timeout)


class Client(ClientEventHandler):
    '''A client for a remote server which handles one or more
:class:`ConnectionPool` of synchronous or asynchronous connections.'''
    connection_pool = ConnectionPool
    '''Factory of :class:`ConnectionPool`.'''
    connection_factory = ClientConnection
    response_factory = None
    '''Factory of response instances'''
    client_version = ''
    connection_pools = None
    timeout = 0
    EVENTS = ('pre_request', 'post_request', 'response')
    
    request_parameters = ('hooks', 'timeout')
    def __init__(self, timeout=None, client_version=None,
                 max_connections=None, trust_env=True, stream=None,
                 **params):
        self.trust_env = trust_env
        self.timeout = timeout if timeout is not None else self.timeout
        self.client_version = client_version or self.client_version
        self.max_connections = max_connections or 2**31
        if self.connection_pools is None:
            self.connection_pools = {}
        self.setup(**params)
    
    def setup(self, **params):
        '''Setup the client. By default it does nothing.'''
    
    def request(self, *args, **params):
        '''Create a request and invoke the :meth:`response` method.
Must be implemented by subclasses.'''
        raise NotImplementedError
    
    def response(self, request, consumer=None):
        '''Once a *request* object has been constructed, the :meth:`request`
method should invoke this method to start the response dance.

:parameter request: A custom request for the :class:`Client`
:parameter consumer: An optional consumer of streaming data, it is the
    :attr:`ClientProtocolConsumer.consumer` attribute of the
    consumer returned by this method.
:rtype: An :class:`ClientProtocolConsumer` obtained form
    :attr:`response_factory`.
'''
        self.fire('pre_request', request)
        # Get a suitable connection pool
        pool = self.connection_pool.get(request,
                                    self.connection_pools,
                                    timeout=request.timeout,
                                    max_connections=self.max_connections,
                                    connection_factory=self.connection_factory)
        # Get or create a connection
        connection = pool.get_or_create_connection(self)
        response = self.response_factory(connection, request, consumer)
        self.fire('post_request', response)
        return response.begin()
    
    def update_parameter(self, params):
        for name in self.request_parameters:
            if name not in params:
                params[name] = getattr(self, name)
            elif name == 'hooks':
                hooks = params[name]
                chooks = dict(((e, copy(h)) for e, h in iteritems(self.hooks)))
                for e, h in iteritems(hooks):
                    chooks[e].append(h)
                params[name] = chooks
        return params
    
    def close_connections(self):
        for p in self.connection_pools.values():
            p.close_connections()
            
    def fire(self, event, *args):
        pass