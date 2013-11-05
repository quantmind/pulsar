'''SqlAlchemy classes for Redis'''
from pulsar import (coroutine_return, ProtocolConsumer, Connection)

import redis

from asyncstore import Pool, Store, run_in_loop, register_store

from .client import Redis, PubSub, Pipeline, RedisParser
from .compiler import RedisCompiler


NOT_DONE = object()


class RedisRequest(ProtocolConsumer):

    def __init__(self, command, args, raise_on_error=True, **options):
        super(RedisRequest, self).__init__()
        self.command = command.upper()
        self.raise_on_error = raise_on_error
        self.args = args
        self.options = options

    def start_request(self):
        conn = self._connection
        self._data_sent = conn.parser.pack_command(self.command, *self.args)
        conn._transport.write(self._data_sent)

    def data_received(self, data):
        parser = self._connection.parser
        parser.feed(data)
        response = parser.get()
        connection = self._connection
        parse = connection.parse_response
        if response is not False:
            if not isinstance(response, Exception):
                response = parse(response, self.command, **self.options)
            elif self.raise_on_error:
                raise response
            self.finished(response)


class RedisTransactionRequest(ProtocolConsumer):

    def __init__(self, args_options, raise_on_error=True):
        self.args_options = deque(args_options)
        self.raise_on_error = raise_on_error
        self.response = []

    def start_request(self):
        conn = self._connection
        self._data_sent = conn.parser.pack_pipeline(self.args_options)
        conn._transport.write(self._data_sent)

    def data_received(self, data):
        parser = self._connection.parser
        parser.feed(data)
        response = parser.get()
        connection = self._connection
        parse = connection.parse_response
        while response is not False:
            args, opts = self.args_options.popleft()
            if not isinstance(response, Exception):
                response = parse(response, args[0], **opts)
            self.response.append(response)
            response = self.parser.get()
        if not self.args_options:
            results = self.response
            if client.transaction:
                results = results[-1]
            try:
                if self.raise_on_error:
                    client.raise_first_error(results)
                return results
            finally:
                client.reset()
        else:
            return NOT_DONE


class RedisConnection(Connection, redis.StrictRedis):

    def __init__(self, session, consumer_factory, producer, timeout=0):
        super(RedisConnection, self).__init__(session, consumer_factory,
                                              producer, timeout)
        self.parser = producer._parser_class()
        self.response_callbacks = redis.StrictRedis.RESPONSE_CALLBACKS.copy()
        self.parser.on_connect(self)

    @property
    def decode_responses(self):
        '''Required by the parser'''
        return self.producer._decode_responses

    @property
    def encoding(self):
        '''Required by the parser'''
        return self.producer._encoding

    def execute(self, command, *args, **options):
        req = self._consumer_factory(command, args, **options)
        self.set_consumer(req)
        req.start(True)
        return req.on_finished

    def parse_response(self, response, command, **options):
        "Parses a response from the Redis server."
        if command in self.response_callbacks:
            return self.response_callbacks[command](response, **options)
        return response


class RedisStore(Store):
    '''Redis :class:`.Store` implementation.
    '''
    def _init(self, namespace=None, parser_class=None, pool_size=50,
              decode_responses=False, **kwargs):
        self._received = 0
        self._decode_responses = decode_responses
        self._parser_class = parser_class or RedisParser
        if namespace:
            self._urlparams['namespace'] = namespace
        self._pool = Pool(self._connect, pool_size=pool_size)

    @property
    def decode_responses(self):
        return self._decode_responses

    @property
    def pool(self):
        return self._pool

    def key(self):
        return (self._dns, self._encoding, self._decode_responses)

    def client(self):
        '''Get a client for the Store'''
        return Redis(self)

    def pipeline(self, transaction=True, shard_hint=None,
                 response_callbacks=None):
        return Pipeline(self, transaction, shard_hint, response_callbacks)

    def pubsub(self, shard_hint=None):
        return PubSub(self, shard_hint)

    def compiler(self):
        '''Return a new :class:`.RedisCompiler`
        '''
        return RedisCompiler(self)

    @run_in_loop
    def connect(self):
        '''Create a new authenticated connection with Redis.

        The connection returned is not part of the connection
        :attr:`pool`.
        '''
        return self._connect()

    @run_in_loop
    def execute(self, command, *args, **options):
        connection = yield self.pool.connect()
        with connection:
            yield connection.execute(command, *args, **options)

    def basekey(self, meta, *args):
        """Calculate the key to access model data.

        :parameter meta: a :class:`stdnet.odm.Metaclass`.
        :parameter args: optional list of strings to prepend to the basekey.
        :rtype: a native string
        """
        namespace = self._urlparams.get('namespace')
        if namespace:
            key = '%s%s' % (namespace, meta.modelkey)
        else:
            key =  meta.modelkey
        postfix = ':'.join((str(p) for p in args if p is not None))
        return '%s:%s' % (key, postfix) if postfix else key

    def _connect(self):
        if isinstance(self._host, tuple):
            host, port = self._host
            transport, connection = yield self.event_loop.create_connection(
                self._new_connection, host, port)
        else:
            raise NotImplementedError
        if self._password:
            yield connection.execute('AUTH', self._password)
        if self._database:
            yield connection.execute('SELECT', self._database)
        coroutine_return(connection)

    def _new_connection(self):
        self._received = session = self._received + 1
        return RedisConnection(session, RedisRequest, self)


register_store('redis',
               'asyncstore.stores.redis.store.RedisStore')
