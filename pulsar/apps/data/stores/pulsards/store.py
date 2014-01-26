from functools import partial

from pulsar import coroutine_return, task, Connection, Pool, get_actor
from pulsar.utils.pep import to_string

from .base import register_store, Store, Command
from .client import Client, Pipeline, Consumer, ResponseError
from .pubsub import PubSub
from ...server import redis_parser


class PulsarStoreConnection(Connection):

    def __init__(self, *args, **kw):
        super(PulsarStoreConnection, self).__init__(*args, **kw)
        self.parser = self._producer._parser_class()

    def execute(self, *args, **options):
        consumer = self.current_consumer()
        consumer.start((args, options))
        return consumer.on_finished

    def execute_pipeline(self, commands, raise_on_error=True):
        consumer = self.current_consumer()
        consumer.start((commands, raise_on_error, []))
        return consumer.on_finished


class PulsarStore(Store):
    '''Pulsar :class:`.Store` implementation.
    '''
    protocol_factory = partial(PulsarStoreConnection, Consumer)
    supported_queries = frozenset(('filter', 'exclude'))

    def _init(self, namespace=None, parser_class=None, pool_size=50,
              decode_responses=False, **kwargs):
        self._decode_responses = decode_responses
        if not parser_class:
            actor = get_actor()
            pyparser = actor.cfg.redis_py_parser if actor else False
            parser_class = redis_parser(pyparser)
        self._parser_class = parser_class
        if namespace:
            self._urlparams['namespace'] = namespace
        self._pool = Pool(self.connect, pool_size=pool_size, loop=self._loop)
        self.loaded_scripts = {}

    @property
    def pool(self):
        return self._pool

    @property
    def namespace(self):
        '''The prefix namespace to append to all transaction on keys
        '''
        n = self._urlparams.get('namespace')
        return '%s:' % n if n else ''

    def key(self):
        return (self._dns, self._encoding)

    def client(self):
        '''Get a client for the Store'''
        return Client(self)

    def pipeline(self):
        '''Get a client for the Store'''
        return Pipeline(self)

    def pubsub(self, protocol=None):
        return PubSub(self, protocol=protocol)

    @task
    def execute(self, *args, **options):
        connection = yield self._pool.connect()
        with connection:
            result = yield connection.execute(*args, **options)
            if isinstance(result, ResponseError):
                raise result.exception
            coroutine_return(result)

    @task
    def execute_pipeline(self, commands, raise_on_error=True):
        conn = yield self._pool.connect()
        with conn:
            result = yield conn.execute_pipeline(commands, raise_on_error)
            if isinstance(result, ResponseError):
                raise result.exception
            coroutine_return(result)

    def connect(self, protocol_factory=None):
        protocol_factory = protocol_factory or self.create_protocol
        if isinstance(self._host, tuple):
            host, port = self._host
            transport, connection = yield self._loop.create_connection(
                protocol_factory, host, port)
        else:
            raise NotImplementedError('Could not connect to %s' %
                                      str(self._host))
        if self._password:
            yield connection.execute('AUTH', self._password)
        if self._database:
            yield connection.execute('SELECT', self._database)
        coroutine_return(connection)

    def execute_transaction(self, transaction):
        '''Execute a :class:`.Transaction`
        '''
        pipe = self.pipeline()
        # loop through models
        for tmodel in transaction.models():
            if tmodel.dirty:
                raise NotImplementedError

        for command in transaction.commands:
            action = command.action
            if not action:
                pipe.execute(*command.args)
            elif action == Command.INSERT:
                model = command.args
                pkvalue = model.pkvalue()
                key = self.basekey(model._meta, pkvalue)
                pipe.hmset(key, model._to_store(self))
            else:
                raise NotImplementedError
        return pipe.commit()

    def get_model(self, model, pk):
        key = '%s%s:%s' % (self.namespace, model._meta.table_name,
                           to_string(pk))
        return self.execute('hgetall', key, factory=model)

    def compile_query(self, query):
        compiled = CompiledQuery(self.pipeline())
        return compiled

    def flush(self):
        return self.execute('flushdb')

    def close(self):
        '''Close all open connections.'''
        return self._pool.close()

    def has_query(self, query_type):
        return query_type in self.supported_queries

    def basekey(self, meta, *args):
        key = '%s%s' % (self.namespace, meta.table_name)
        postfix = ':'.join((to_string(p) for p in args if p is not None))
        return '%s:%s' % (key, postfix) if postfix else key

    def meta(self, meta):
        '''Extract model metadata for lua script stdnet/lib/lua/odm.lua'''
        indices = dict(((idx.attname, idx.unique) for idx in meta.indices))
        data = meta.as_dict()
        data['namespace'] = self.basekey(meta)
        return data


class CompiledQuery(object):

    def __init__(self, pipe, query):
        self.pipe = pipe


register_store('pulsar',
               'pulsar.apps.data.stores.pulsards.store.PulsarStore')
