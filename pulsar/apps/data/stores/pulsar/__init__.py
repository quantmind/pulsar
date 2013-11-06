from pulsar import coroutine_return, in_loop_thread, ProtocolConsumer
from pulsar.apps.data import (register_store, Store, Pool, RedisParser,
                              PulsarStoreConnection)


class Request(object):
    _data_sent = None

    def __init__(self, command, args, raise_on_error=True, **options):
        self.command = command.upper()
        self.raise_on_error = raise_on_error
        self.args = args
        self.options = options

    def write(self, consumer):
        conn = consumer._connection
        self._data_sent = conn.parser.pack_command(self.command, *self.args)
        conn._transport.write(self._data_sent)

    def data_received(self, consumer, data):
        conn = consumer._connection
        parser = conn.parser
        parser.feed(data)
        response = parser.get()
        if response is not False:
            if not isinstance(response, Exception):
                response = self.parse_response(response)
            elif self.raise_on_error:
                raise response
            consumer.finished(response)

    def parse_response(self, response):
        return response


class Consumer(ProtocolConsumer):

    def start_request(self):
        self._request.write(self)

    def data_received(self, data):
        self._request.data_received(self, data)


class PulsarClient(object):

    def __init__(self, store):
        self.store = store

    def get(self, key):
        return self.execute_command('get', key)

    def set(self, key, value, timeout=None):
        pieces = [key, value]
        if timeout:
            pieces.append(timeout)
        return self.execute_command('set', *pieces)

    def ping(self):
        return self.execute_command('ping')

    def execute_command(self, command, *args, **options):
        return self.store.execute(command, *args, **options)


class PulsarStore(Store):
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
    def pool(self):
        return self._pool

    def key(self):
        return (self._dns, self._encoding)

    def client(self):
        '''Get a client for the Store'''
        return PulsarClient(self)

    @in_loop_thread
    def execute(self, command, *args, **options):
        connection = yield self._pool.connect()
        with connection:
            consumer = connection.current_consumer()
            consumer.start(Request(command, args, **options))
            result = yield consumer.on_finished
            coroutine_return(result)

    def _connect(self):
        if isinstance(self._host, tuple):
            host, port = self._host
            transport, connection = yield self._loop.create_connection(
                self._new_connection, host, port)
        else:
            raise NotImplementedError
        coroutine_return(connection)

    def _new_connection(self):
        self._received = session = self._received + 1
        return PulsarStoreConnection(self._parser_class, Consumer,
                                     session=session, producer=self)


register_store('pulsar',
               'pulsar.apps.data.stores.pulsar.PulsarStore')
