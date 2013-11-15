from pulsar import (coroutine_return, in_loop_thread, Connection,
                    ProtocolConsumer, Pool)
from pulsar.utils.pep import native_str, zip

from .base import register_store, Store
from .client import Client, Pipeline
from .pubsub import PubSub
from ...server import redis_parser


def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged


def pairs_to_object(response, factory=None):
    it = iter(response)
    return (factory or dict)(zip(it, it))


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


def parse_info(response):
    info = {}
    response = native_str(response)

    def get_value(value):
        if ',' not in value or '=' not in value:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}
            for item in value.split(','):
                k, v = item.rsplit('=', 1)
                sub_dict[k] = get_value(v)
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith('#'):
            key, value = line.split(':', 1)
            info[key] = get_value(value)
    return info


class PulsarStoreConnection(Connection):
    '''Used both by client and server'''
    def __init__(self, *args, **kw):
        super(PulsarStoreConnection, self).__init__(*args, **kw)
        self.parser = self._producer._parser_class()

    def execute(self, command, *args, **options):
        consumer = self.current_consumer()
        consumer.start(Request(command, args, **options))
        return consumer.on_finished

    def execute_pipeline(self, commands, raise_on_error):
        consumer = self.current_consumer()
        consumer.start(PipelineRequest(commands, raise_on_error))
        return consumer.on_finished


class Request(object):
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'FLUSHALL FLUSHDB HMSET LSET LTRIM MSET RENAME '
            'SAVE SELECT SHUTDOWN SLAVEOF SET WATCH UNWATCH',
            lambda r: r == b'OK'
        ),
        {
         'PING': lambda r: r == b'PONG',
         'INFO': parse_info,
         'TIME': lambda x: (int(x[0]), int(x[1])),
         'HGETALL': pairs_to_object
         }
    )

    def __init__(self, command, args, **options):
        self.command = command.upper()
        self.args = args
        self.options = options

    def write(self, consumer):
        conn = consumer._connection
        chunk = conn.parser.multi_bulk(self.command, *self.args)
        conn._transport.write(chunk)
        return chunk

    def data_received(self, consumer, data):
        conn = consumer._connection
        parser = conn.parser
        parser.feed(data)
        response = parser.get()
        if response is not False:
            if not isinstance(response, Exception):
                response = self.parse_response(response, self.command,
                                               self.options)
                if response is None:
                    consumer.bind_event('post_request', lambda r: None)
            else:
                raise response
            consumer.finished(response)

    def parse_response(self, response, command, options):
        callback = self.RESPONSE_CALLBACKS.get(command.upper())
        return callback(response, **options) if callback else response


class PipelineRequest(Request):

    def __init__(self, commands, raise_on_error):
        self.commands = commands
        self.responses = []
        self.raise_on_error = raise_on_error

    def write(self, consumer):
        conn = consumer._connection
        chunk = conn.parser.pack_pipeline(self.commands)
        conn._transport.write(chunk)
        return chunk

    def data_received(self, consumer, data):
        conn = consumer._connection
        parser = conn.parser
        parser.feed(data)
        response = parser.get()
        while response is not False:
            if isinstance(response, Exception) and self.raise_on_error:
                raise response
            self.responses.append(response)
            response = parser.get()
        if len(self.responses) == len(self.commands):
            response = []
            error = None
            for cmds, resp in zip(self.commands[1:-1], self.responses[-1]):
                args, options = cmds
                if isinstance(resp, Exception) and not error:
                    error = resp
                response.append(self.parse_response(resp, args[0], options))
            if error and self.raise_on_error:
                raise error
            consumer.finished(response)


class Consumer(ProtocolConsumer):

    def start_request(self):
        self._request.write(self)

    def data_received(self, data):
        self._request.data_received(self, data)


class PulsarStore(Store):
    '''Pulsar :class:`.Store` implementation.
    '''
    def _init(self, namespace=None, parser_class=None, pool_size=50,
              decode_responses=False, **kwargs):
        self._received = 0
        self._decode_responses = decode_responses
        self._parser_class = parser_class or redis_parser()
        if namespace:
            self._urlparams['namespace'] = namespace
        self._pool = Pool(self.connect, pool_size=pool_size)
        self.loaded_scripts = {}

    @property
    def pool(self):
        return self._pool

    def key(self):
        return (self._dns, self._encoding)

    def client(self):
        '''Get a client for the Store'''
        return Client(self)

    def pipeline(self):
        '''Get a client for the Store'''
        return Pipeline(self)

    def pubsub(self):
        return PubSub(self)

    @in_loop_thread
    def execute(self, command, *args, **options):
        connection = yield self._pool.connect()
        with connection:
            result = yield connection.execute(command, *args, **options)
            coroutine_return(result)

    @in_loop_thread
    def execute_pipeline(self, commands, raise_on_error=True):
        conn = yield self._pool.connect()
        with conn:
            result = yield conn.execute_pipeline(commands, raise_on_error)
            coroutine_return(result)

    def connect(self, protocol_factory=None):
        protocol_factory = protocol_factory or self._new_connection
        if isinstance(self._host, tuple):
            host, port = self._host
            transport, connection = yield self._loop.create_connection(
                protocol_factory, host, port)
        else:
            raise NotImplementedError
        if self._password:
            yield connection.execute('AUTH', self._password)
        if self._database:
            yield connection.execute('SELECT', self._database)
        coroutine_return(connection)

    def _new_connection(self):
        self._received = session = self._received + 1
        return PulsarStoreConnection(Consumer, session=session, producer=self)


register_store('pulsar',
               'pulsar.apps.data.stores.pulsar.store.PulsarStore')
