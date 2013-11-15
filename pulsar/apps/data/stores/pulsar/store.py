from itertools import chain

from pulsar import (coroutine_return, in_loop_thread, Connection,
                    ProtocolConsumer, Pool)
from pulsar.utils.pep import native_str

from .base import register_store, Store, Queue
from .pubsub import PubSub
from ...server import redis_parser


def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged


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

    def execute_pipeline(self, commands):
        consumer = self.current_consumer()
        consumer.start(PipelineRequest(commands))
        return consumer.on_finished


class Request(object):
    RESPONSE_CALLBACKS = dict_merge(
        {
         'PING': lambda r: r == b'PONG',
         'SET': lambda r: r == b'OK',
         'INFO': parse_info,
         'TIME': lambda x: (int(x[0]), int(x[1])),
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
                                               **self.options)
                if response is None:
                    consumer.bind_event('post_request', lambda r: None)
            else:
                raise response
            consumer.finished(response)

    def parse_response(self, response, command, options):
        callback = self.RESPONSE_CALLBACKS.get(command)
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
            for cmds, response in zip(self.commands[1:-1], self.responses[-1]):
                args, options = cmds
                response.append(self.parse_response(response, args[0],
                                                    **options))
            consumer.finished(response)


class Consumer(ProtocolConsumer):

    def start_request(self):
        self._request.write(self)

    def data_received(self, data):
        self._request.data_received(self, data)


class PulsarQueue(Queue):

    def get(self, timeout=None):
        return self.store.execute('brpop', self.id)

    def put(self, item):
        return self.store.execute('lpush', item)


class Client(object):

    def __init__(self, store):
        self.store = store

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.store)
    __str__ = __repr__

    def pubsub(self):
        return PubSub(self.store)

    def pipeline(self):
        return Pipeline(self.store)

    def execute(self, command, *args, **options):
        return self.store.execute(command, *args, **options)
    execute_command = execute


class Pipeline(Client):

    def __init__(self, store):
        self.store = store
        self.reset()

    def execute(self, *args, **kwargs):
        self.command_stak.append((args, kwargs))
    execute_command = execute

    def execute_command(self, command, *args, **options):
        return self.store.execute(command, *args, **options)

    def reset(self):
        self.command_stack = []
        self.scripts = set()

    def commit(self, raise_on_error=True):
        cmds = list(chain([(('MULTI', ), {})],
                          self.command_stack, [(('EXEC', ), {})]))
        self.reset()
        return self.store.execute_pipeline(cmds)


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

    def queue(self, id):
        '''Create a districuted queue'''
        return PulsarQueue(self, id)

    @in_loop_thread
    def execute(self, command, *args, **options):
        connection = yield self._pool.connect()
        with connection:
            result = yield connection.execute(command, *args, **options)
            coroutine_return(result)

    @in_loop_thread
    def execute_pipeline(self, commands):
        connection = yield self._pool.connect()
        with connection:
            result = yield connection.execute_pipeline(commands)
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
