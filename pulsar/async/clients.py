from pulsar.utils.pep import get_event_loop
from pulsar.utils.sockets import SOCKET_TYPES, create_socket

from .defer import Deferred
from .protocols import ProtocolConsumer, Connection
from .transports import Transport
from .servers import Producer, EventHandler

__all__ = ['create_connection', 'ConnectionPool', 'Client',
           'ProtocolConsumer']

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
    
    
class ClientProtocolConsumer(ProtocolConsumer):
    '''A :class:`ProtocolConsumer` for a :class:`Client`.
    
.. attribute:: protocol


.. attribute:: request

    The request sent to the remote server
    
.. attribute:: consumer

    Optional consumer of data received from the server in response to
    :attr:`request`. This can be used to stream data as it arrives (for example)
'''
    def __init__(self, connection, request, consumer):
        super(ClientProtocolConsumer, self).__init__(connection)
        self.request = request
        self.consumer = consumer
        self.when_ready = Deferred()
        
    def begin(self):
        self.protocol.on_connection.add_callback(self.send, self.close)
        return self
    
    def feed(self, data):
        try:
            msg, data = self.decode(data)
            if msg:
                self._finished = True
                if data:
                    raise ProtocolError
                self.when_ready.callback(msg)
        except Exception as e:
            self.when_ready.callback(e)
            
    def send(self, res):
        msg = request.encode()
        self.transport.write(msg)
    
    def decode(self):
        raise NotImplementedError
    
    def close(self):
        pass
        
        
class ClientConnection(Connection):
    
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
            self._producer.fire('response', self._current_response)
            self._current_response = None
        else:
            raise RuntimeError()
    
        
class ConnectionPool(Producer):
    '''A :class:`Producer` of of active connections for client
protocols. It maintains a live set of connections.

.. attribute:: address

    Address to connect to
    
.. attribute:: all

    A class attribute containing all active :class:`ConnectionPool`
    '''
    all = {}
    
    def __init__(self, request, **params):
        super(ConnectionPool, self).__init__(**params)
        self._address = request.address
        self._available_connections = []
        
    @classmethod
    def get(cls, request, **params):
        self = cls(request, **params)
        if self not in cls.all:
            cls.all[self] = self
        return cls.all[self]
    
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
        
    def get_or_create_connection(self, response_factory):
        "Get or create a new connection from the pool"
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = None
        else:
            # we have a connection, lets added it to the concurrent set
            self._concurrent_connections.add(connection)
        if connection is None:
            # build protocol and build the new connection
            protocol = self.build_protocol(response_factory)
            connection = self.new_connection(protocol, response_factory)
        return connection
    
    def build_protocol(self, response_factory):
        return create_connection(self._address, timeout=self._timeout)
    
    
class Request(ClientEventHandler):
    '''A :class:`Client` request class.'''
    def __init__(self, address, timeout=0):
        self.address = address
        self.timeout = timeout


class Client(ClientEventHandler):
    '''A client for a remote server which handles one or more
:class:`ConnectionPool` of synchronous or asynchronous connections.'''
    connection_pool = ConnectionPool
    '''Factory of :class:`ConnectionPool`.'''
    connection_factory = ClientConnection
    response_factory = None
    '''Factory of response instances'''
    client_version = ''
    stream = False
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
        self.stream = stream if stream is not None else self.stream
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
:parameter consumer: An optional consumer of streaming data.
:rtype: An object obtained form :attr:`response_factory`.
'''
        self.fire('pre_request', request)
        pool = self.connection_pool.get(request,
                                    timeout=request.timeout,
                                    max_connections=self.max_connections,
                                    connection_factory=self.connection_factory)
        connection = pool.get_or_create_connection(self.response_factory)
        response = self.response_factory(connection, request, consumer)
        self.fire('post_request', response.request)
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
            
    def fire(self, event, *args):
        pass