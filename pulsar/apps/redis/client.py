import pulsar


NOT_DONE = object()


class RedisRequest(pulsar.Request):
    '''Asynchronous Request for redis.'''
    def __init__(self, client, connection, timeout, encoding,
                 encoding_errors, command_name, args, parser,
                 raise_on_error=True, on_finished=None,
                 release_connection=True, **options):
        self.client = client
        self.connection = connection
        self.timeout = timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.command_name = command_name.upper()
        self.parser = parser
        self.raise_on_error = raise_on_error
        self.on_finished = on_finished
        self.release_connection = release_connection
        self.args = args
        self.last_response = False
        pool = client.connection_pool
        if client.is_pipeline:            
            self.response = []
            self.command = pool.pack_pipeline(args)
            self.args_options = deque(args)
        else:
            self.command = pool.pack_command(self.command_name, *args)
            self.options = options
        
    @property
    def key(self):
        return (self.connection, self.timeout)
    
    @property
    def address(self):
        return self.connection.address
    
    @property
    def is_pipeline(self):
        return not bool(self.command_name)
    
    def __repr__(self):
        if self.is_pipeline:
            return 'PIPELINE%s' % self.args
        else:
            return '%s%s' % (self.command_name, self.args)
    __str__ = __repr__
    
    def read_response(self):
        # For compatibility with redis-py
        return self.last_response
    
    def feed(self, data):
        self.parser.feed(data)
        response = self.parser.get()
        c = self.client
        parse = c.parse_response
        if c.is_pipeline:
            while response is not False:
                self.last_response = response
                args, opts = self.args_options.popleft()
                if not isinstance(response, Exception):
                    response = parse(self, args[0], **opts)
                self.response.append(response)
                response = self.parser.get()
            if not self.args_options:
                return c.on_response(self.response, self.raise_on_error)
            else:
                return NOT_DONE
        else:
            result = NOT_DONE
            while response is not False:
                self.last_response = result = response
                if not isinstance(response, Exception):
                    result = parse(self, self.command_name, **self.options)
                elif self.raise_on_error:
                    raise response
                response = self.parser.get()
            return result
        
    
class RedisProtocol(pulsar.ProtocolConsumer):
    '''An asynchronous pulsar protocol for redis.'''
    parser = None
    release_connection = True
    
    def data_received(self, data):
        response = self._request.feed(data)
        if response is not NOT_DONE:
            on_finished = self._request.on_finished
            if on_finished and not on_finished.done():
                on_finished.callback(response)
            elif self.release_connection:
                self.finished(response)
    
    def start_request(self):
        # If this is the first request and the connection is new do
        # the login/database switch
        if self.connection.processed <= 1 and self.request_processed == 1:
            request = self._request
            reqs = []
            client = request.client
            if client.is_pipeline:
                client = client.client
            producer = self.producer
            c = producer.connection
            if producer.password:
                reqs.append(producer._new_request(client,
                        'auth', (producer.password,), on_finished=Deferred()))
            if c.db:
                reqs.append(producer._new_request(client,
                        'select', (c.db,), on_finished=Deferred()))
            reqs.append(request)
            for req, next in zip(reqs, reqs[1:]):
                req.on_finished.add_callback(partial(self._next, next))
            self._request = reqs[0]
        self.transport.write(self._request.command)
        
    def _next(self, request, r):
        return self.new_request(request)

    
class AsyncConnectionPoolBase(pulsar.Client):
    '''A :class:`pulsar.Client` for managing a connection pool with redis
data-structure server.'''
    connection_pools = {}
    consumer_factory = RedisProtocol
    
    def __init__(self, address, db=0, password=None, encoding=None, parser=None,
                 encoding_errors='strict', **kwargs):
        super(AsyncConnectionPoolBase, self).__init__(**kwargs)
        self.encoding = encoding or 'utf-8'
        self.encoding_errors = encoding_errors or 'strict'
        self.password = password
        self._setup(address, db, parser)
    
    def pubsub(self, shard_hint=None):
        return PubSub(self, shard_hint)
    
    def request(self, client, command_name, *args, **options):
        response = options.pop('consumer', None)
        full_response = options.pop('full_response', False)
        request = self._new_request(client, command_name, args, **options)
        response = self.response(request, response, False)
        return response if full_response else response.on_finished
    
    def request_pipeline(self, pipeline, raise_on_error=True):
        commands = pipeline.command_stack
        if not commands:
            return ()
        if pipeline.is_transaction:
            commands = list(chain([(('MULTI', ), {})], commands,
                                  [(('EXEC', ), {})]))
        request = self._new_request(pipeline, '', commands,
                                    raise_on_error=raise_on_error)
        return self.response(request).on_finished
    
    def _new_request(self, client, command_name, args, **options):
        return AsyncRedisRequest(client, self.connection, self.timeout,
                                 self.encoding, self.encoding_errors,
                                 command_name, args, self.redis_parser(),
                                 **options)
    
    def _next(self, consumer, next_request, result):
        consumer.new_request(next_request)
        
    def execute_script(self, client, to_load, callback):
        # Override execute_script so that we execute after scripts have loaded
        if to_load:
            results = []
            for name in to_load:
                s = get_script(name)
                results.append(client.script_load(s.script))
            return multi_async(results).add_callback(callback)
        else:
            return callback()
        
