from functools import partial

from asyncio_mongo.database import Database
from asyncio_mongo import protocol


from pulsar import Protocol, Pool, task, coroutine_return

from ..pulsards import register_store, Store


class MongoProtocol(protocol.MongoProtocol):

    def __init__(self, session=1, producer=None, timeout=0):
        self._session = session
        self._timeout = timeout
        self._producer = producer
        super(MongoProtocol, self).__init__()

    @property
    def sock(self):
        '''The socket of :attr:`transport`.
        '''
        if self.transport:
            return self.transport.get_extra_info('socket')


class MongoDbStore(Store):
    '''Pulsar :class:`.Store` implementation.
    '''
    protocol_factory = MongoProtocol

    def _init(self, pool_size=10, **kwargs):
        self._pool = Pool(self.connect, pool_size=pool_size, loop=self._loop)

    @task
    def database(self, name=None):
        connection = yield self._pool.connect()
        coroutine_return(Database(connection, name or self._database))

    @task
    def execute(self, *args, **options):
        connection = yield self._pool.connect()
        with connection:
            result = yield self._execute(connection, *args, **options)
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
        if self._database and self._password:
            db = Database(connection, self._database)
            yield db.authenticate(self._username, self._password)
        coroutine_return(connection)

    def _execute(self, connection, *args, **kwargs):
        pass


register_store('mongodb',
               'pulsar.apps.data.stores.mongodb.store.MongoDbStore')
