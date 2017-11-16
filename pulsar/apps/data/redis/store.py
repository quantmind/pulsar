from functools import partial

from ....async.clients import Pool
from ....utils.string import to_string
from ..store import RemoteStore

from .client import RedisClient, Pipeline, Consumer, RedisStoreConnection
from .pubsub import RedisPubSub, RedisChannels


class RedisStore(RemoteStore):
    '''Redis :class:`.Store` implementation.
    '''
    supported_queries = frozenset(('filter', 'exclude'))

    def _init(self, namespace=None, pool_size=10,
              decode_responses=False, **kwargs):
        self.protocol_factory = partial(RedisStoreConnection, Consumer)
        self._decode_responses = decode_responses
        if namespace:
            self._urlparams['namespace'] = namespace
        self._pool = Pool(self.connect, pool_size=pool_size, loop=self._loop)
        if self._database is None:
            self._database = 0
        self._database = int(self._database)
        self.loaded_scripts = set()

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
        '''Get a :class:`.RedisClient` for the Store'''
        return RedisClient(self)

    def pipeline(self):
        '''Get a :class:`.Pipeline` for the Store'''
        return Pipeline(self)

    def pubsub(self, protocol=None):
        return RedisPubSub(self, self.protocol_factory, protocol=protocol)

    def channels(self, protocol=None, **kw):
        return RedisChannels(self.pubsub(protocol=protocol), **kw)

    def ping(self):
        return self.client().ping()

    async def execute(self, *args, **options):
        connection = await self._pool.connect()
        async with connection:
            result = await connection.execute(*args, **options)
            return result

    async def execute_pipeline(self, commands, raise_on_error=True):
        conn = await self._pool.connect()
        async with conn:
            result = await conn.execute_pipeline(commands, raise_on_error)
            return result

    async def connect(self, protocol_factory=None):
        protocol_factory = protocol_factory or self.create_protocol
        if isinstance(self._host, tuple):
            host, port = self._host
            transport, connection = await self._loop.create_connection(
                protocol_factory, host, port)
        else:
            raise NotImplementedError('Could not connect to %s' %
                                      str(self._host))
        if self._password:
            await connection.execute('AUTH', self._password)
        if self._database:
            await connection.execute('SELECT', self._database)
        return connection

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
        #  indices = dict(((idx.attname, idx.unique) for idx in meta.indices))
        data = meta.as_dict()
        data['namespace'] = self.basekey(meta)
        return data


class CompiledQuery:

    def __init__(self, pipe, query):
        self.pipe = pipe
