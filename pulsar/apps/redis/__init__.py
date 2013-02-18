'''Asynchronous Redis client.
'''
from collections import namedtuple

import pulsar
from pulsar.utils.pep import ispy3k, map
try:
    from .api import StrictRedis
except ImportError:
    StrictRedis = None

redis_connection = namedtuple('redis_connection', 'address db password charset')    

class RedisRequest(object):

    def __init__(self, client, connection, timeout,
                 command_name, args, options):
        self.client = client
        self.connection = connection
        self.timeout = timeout
        self.command_name = command_name
        self.args = args
        self.options = options
        if command_name:
            self.command = self.pack_command(command_name, *args)
        else:
            self.response = []
            self.command = self.pack_pipeline(args)
        
    @property
    def key(self):
        return self.connection
    
    @property
    def address(self):
        return self.connection.address
    
    def __repr__(self):
        if self.command_name:
            return '%s%s' % (self.command_name, self.args)
        else:
            return 'PIPELINE{0}' % (self.args)
    __str__ = __repr__
        
    def feed(self, data):
        parser = self.parser
        parser.feed(data)
        if self.is_pipeline:
            while 1:
                response = parser.gets()
                if response is False:
                    break
                self.response.append(response)
            if len(self.response) == self.num_responses:
                return self.close()
        else:
            self.response = parser.gets()
            if self.response is not False:
                return self.close()
    
    if ispy3k:
        def encode(self, value):
            return value if isinstance(value, bytes) else str(value).encode(
                                        self.connection.charset)
            
    else:   #pragma    nocover
        def encode(self, value):
            if isinstance(value, unicode):
                return value.encode(self.connection.charset)
            else:
                return str(value)
            
    def __pack_gen(self, args):
        e = self.encode
        crlf = b'\r\n'
        yield e('*%s\r\n'%len(args))
        for value in map(e, args):
            yield e('$%s\r\n'%len(value))
            yield value
            yield crlf
    
    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        return b''.join(self.__pack_gen(args))
    
    
class RedisProtocol(pulsar.ProtocolConsumer):
    
    def __init__(self, connection=None):
        super(RedisProtocol, self).__init__(connection=connection)
        self.chained_requests = deque()
        
    def start_request(self):
        pass
    
    def data_received(self, data):
        if self.current_request.feed(data):
            # The request has finished
            if self.chained_requests:
                req = self.chained_requests.popleft()
                self.new_request(req)
            else:
                self.finished()
                
    
class RedisClient(pulsar.Client):
    '''A :class:`pulsar.Client` for managing a connection pool with redis
data-structure server.'''
    connection_pools = {}
    '''The charset to encode redis commands'''
    consumer_factory = RedisProtocol
    
    def __init__(self, address, db=0, password=None, charset=None, **kwargs):
        super(RedisClient, self).__init__(**kwargs)
        charset = charset or 'utf-8'  
        self._connection = redis_connection(address, db, password, charset)
    
    def request(self, client, command_name, *args, **options):
        request = self._new_request(client, command_name, *args, **options)
        return self.response(request)
    
    def response(self, request):
        connection = self.get_connection(request)
        consumer = self.consumer_factory(conn)
        # If this is a new connection we need to select database and login
        if not connection.processed:
            c = self._connection
            if c.password:
                req = self._new_request(request.client, 'auth', c.password)
                consumer.chained_requests.append(req)
            if c.db:
                req = self._new_request(request.client, 'select', c.db)
                consumer.chained_requests.append(req)
            consumer.chained_requests.append(request)
            request = consumer.chained_requests.popleft()
        consumer.new_request(request)
        return consumer
            
    def _new_request(self, client, command, *args, **options):
        return RedisRequest(client, self._connection, self.timeout,
                            command, args, options)