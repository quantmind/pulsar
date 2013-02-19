'''Asynchronous connector for redis-py.

Usage::

    from pulsar.apps.redis import RedisClient
    
    client = RedisClient('127.0.0.1:6349')
    pong = yield client.ping()
'''
from collections import namedtuple, deque

import pulsar
from pulsar import ProtocolError
from pulsar.utils.pep import ispy3k, map
try:
    from .api import StrictRedis, ResponseError
except ImportError:
    StrictRedis = None
    class ResponseError(Exception):
        pass

from .reader import *

redis_connection = namedtuple('redis_connection', 'address db password charset')    


class RedisRequest(object):

    def __init__(self, client, connection, timeout,
                 command_name, args, options):
        self.client = client
        self.connection = connection
        self.timeout = timeout
        self.command_name = command_name.upper()
        self.args = args
        self.options = options
        self.reader = RedisReader(ProtocolError, ResponseError)
        if command_name:
            self.response = None
            self.command = self.pack_command(command_name, *args)
        else:
            self.response = []
            self.command = self.pack_pipeline(args)
        
    @property
    def key(self):
        return (self.connection, self.timeout)
    
    @property
    def address(self):
        return self.connection.address
    
    @property
    def is_pipeline(self):
        return isinstance(self.response, list)
    
    def __repr__(self):
        if self.command_name:
            return '%s%s' % (self.command_name, self.args)
        else:
            return 'PIPELINE{0}' % (self.args)
    __str__ = __repr__
    
    def read_response(self):
        # For compatibility with redis-py
        return self.response
    
    def feed(self, data):
        self.reader.feed(data)
        if self.is_pipeline:
            while 1:
                response = parser.gets()
                if response is False:
                    break
                self.response.append(response)
            if len(self.response) == self.num_responses:
                return self.close()
        else:
            self.response = self.reader.gets()
            if self.response is not False:
                return self.close()
    
    def close(self):
        c = self.client
        response = c.parse_response(self, self.command_name, **self.options)
        if isinstance(response, Exception):
            raise response
        return response
    
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
        self.all_requests = []
        
    def chain_request(self, request):
        self.all_requests.append(request)
        
    def new_request(self, request=None):
        if request is None:
            self._requests = deque(self.all_requests)
            request = self._requests.popleft()
        return super(RedisProtocol, self).new_request(request)
    
    def start_request(self):
        self.transport.write(self.current_request.command)
    
    def data_received(self, data):
        result = self.current_request.feed(data)
        if result is not None:
            # The request has finished
            if self._requests:
                self.new_request(self._requests.popleft())
            else:
                self.finished(result)
                
    
class RedisClient(pulsar.Client):
    '''A :class:`pulsar.Client` for managing a connection pool with redis
data-structure server.'''
    connection_pools = {}
    '''The charset to encode redis commands'''
    consumer_factory = RedisProtocol
    
    def __init__(self, address, db=0, password=None, charset=None, **kwargs):
        super(RedisClient, self).__init__(**kwargs)
        charset = charset or 'utf-8'
        self._connection = redis_connection(address, int(db), password, charset)
    
    def request(self, client, command_name, *args, **options):
        request = self._new_request(client, command_name, *args, **options)
        return self.response(request)
    
    def response(self, request):
        connection = self.get_connection(request)
        consumer = self.consumer_factory(connection)
        # If this is a new connection we need to select database and login
        if not connection.processed:
            c = self._connection
            if c.password:
                req = self._new_request(request.client, 'auth', c.password)
                consumer.chain_request(req)
            if c.db:
                req = self._new_request(request.client, 'select', c.db)
                consumer.chain_request(req)
        consumer.chain_request(request)
        consumer.new_request()
        return consumer.on_finished
            
    def _new_request(self, client, command, *args, **options):
        return RedisRequest(client, self._connection, self.timeout,
                            command, args, options)