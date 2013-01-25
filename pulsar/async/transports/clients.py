from pulsar.utils.pep import get_event_loop
from pulsar.utils.sockets import get_transport_type, create_socket

from .protocols import ProtocolConsumer, Connection, EventHandler, Producer, NOTHING
from .transport import create_transport

__all__ = ['ConnectionPool',
           'Client',
           'ClientProtocolConsumer',
           'Request']

    
class Request(EventHandler):
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
    def __init__(self, connection, request, consumer=None):
        super(ClientProtocolConsumer, self).__init__(connection)
        connection.set_response(self)
        self.request = request
        if consumer:
            self.bind_event('data_received', consumer)
        
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
        except Exception:
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
            protocol = client.build_protocol(self._address, self._timeout)
            connection = self.new_connection(protocol, client.consumer_factory,
                                             client)
        return connection
    

class Client(EventHandler):
    '''A client for a remote server which handles one or more
:class:`ConnectionPool` of asynchronous connections.

.. attribute:: timeout

    timeout in seconds for an idle connection to be dropped. A value of zero
    means no timeout.
'''
    connection_pool = ConnectionPool
    '''Factory of :class:`ConnectionPool`.'''
    consumer_factory = None
    '''A factory of :class:`ProtocolConsumer` for sending and consuming data.'''
    connection_factory = Connection
    '''A factory of :class:`Connection`.'''
    client_version = ''
    timeout = 0
    
    MANY_TIMES_EVENTS = ('pre_request', 'post_request')
    request_parameters = ('timeout',)
    def __init__(self, max_connections=None, timeout=None, client_version=None,
                 trust_env=True, consumer_factory=None,**params):
        super(Client, self).__init__()
        self.trust_env = trust_env
        self.client_version = client_version or self.client_version
        self.timeout = timeout if timeout is not None else self.timeout
        if consumer_factory:
            self.consumer_factory = consumer_factory
        self.max_connections = max_connections or 2**31
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
    :attr:`consumer_factory`.
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
        response = self.consumer_factory(connection, request, consumer)
        self.fire('post_request', response)
        return response.begin()
    
    def update_parameters(self, params):
        '''Update *param* with attributes of this :class:`Client` defined
in :attr:`request_parameters` tuple.'''
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
    
    def create_connection(self, address, timeout=0):
        '''Create a new connection'''
        transport = create_transport(address=address, timeout=timeout)
        transport.connect()
        connection = self.connection_factory(address, self)
        
    def close_connections(self):
        for p in self.connection_pools.values():
            p.close_connections()
            
    def close(self):
        self.close_connections()