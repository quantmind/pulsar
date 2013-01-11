from pulsar.utils.pep import get_event_loop

from .protocols import ClientResponse
from .tcp import TCPClient

__all__ = ['create_connection', 'ClientSessions']


def create_connection(address, timeout=0, streaming=False, source_address=None,
                      backlog=None):
    '''Create a connection with a remote server'''
    sock = create_socket(address=address, backlog=backlog)
    sock.settimeout(timeout)
    if source_address:
        sock.bind(source_address)
    if sock.type == 'tcp' or sock.type == 'unix':
        protocol_type = TCPClient
    else:
        raise NotImplemented
    protocol = protocol_type(address)
    transport_class = StreamClientTransport if streaming else ClientTransport
    transport = transport_class(get_event_loop(), sock, protocol)
    return transport.connect()
    

class ConnectionPool(object):
    all = {}
    
    @classmethod
    def get(cls, address, timeout, streaming=False, **params):
        self = cls()
        self._address = address
        self._timeout = timeout
        self._streaming = streaming
        self.setup(**params)
        if self not in cls.all:
            self._in_use_connections = set()
            self._available_connections = []
            cls.all[self] = self
        return cls.all[self]
    
    def setup(self):
        pass
    
    def __hash__(self):
        return hash((self.address, self.db, self.timeout))
    
    @property
    def address(self):
        return self._address
    
    def release(self, connection):
        "Releases the connection back to the pool"
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

    def remove(self, connection):
        '''Remove the *connection* from the pool'''
        self._in_use_connections.remove(connection)
        try:
            connection.close()
        except:
            pass
        
    def get_connection(self):
        "Get a connection from the pool"
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = create_connection(self._address, self._timeout,
                                           self._streaming)
        self._in_use_connections.add(connection)
        return connection
        

class ClientSessions(object):
    '''A client for a server which handles a pool of synchronous
or asynchronous connections.'''
    connection_pool = ConnectionPool
    response_class = ClientResponse
    client_version = ''
    stream = False
    timeout = 0
    
    request_parameters = ('hooks', 'timeout', 'stream')
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
        '''Setup the client sessions handler. By default it does nothing.'''
    
    def build_request(self, *args, **kwargs):
        raise NotImplementedError
    
    def request(self, *args, **params):
        '''Build a request object to pass to the :attr:`response_class` which
build the response for the request.'''
        for parameter in self.request_parameters:
            self._update_parameter(parameter, params)
        request = self.build_request(*args, **params)
        pool = self.connection_pool.get(request)
        connection = pool.get_connection(request)
        response = self.response_class(connection, request)
        events.fire('pre_request', response)
        return response
    
    def _update_parameter(self, name, params):
        if name not in params:
            params[name] = getattr(self, name)
        elif name == 'hooks':
            hooks = params[name]
            chooks = dict(((e, copy(h)) for e, h in iteritems(self.hooks)))
            for e, h in iteritems(hooks):
                chooks[e].append(h)
            params[name] = chooks