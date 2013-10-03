'''
Redis
~~~~~~~~~~~~~~~

.. autoclass:: Redis
   :members:
   :member-order: bysource

Pub/Sub
~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource
'''
import sys
from collections import deque

import redis
from redis.exceptions import NoScriptError, InvalidResponse
from redis.client import BasePipeline as _BasePipeline
from redis.connection import PythonParser as _p

import pulsar
from pulsar import Deferred, ProtocolError
from pulsar.utils.pep import zip

from .parser import Parser

EXCEPTION_CLASSES = _p.EXCEPTION_CLASSES


def ResponseError(response):
    "Parse an error response"
    response = response.split(' ')
    error_code = response[0]
    if error_code not in EXCEPTION_CLASSES:
        error_code = 'ERR'
    response = ' '.join(response[1:])
    return EXCEPTION_CLASSES[error_code](response)


RedisParser = lambda: Parser(InvalidResponse, ResponseError)
HAS_C_EXTENSIONS = True

try:
    from . import cparser
    CRedisParser = lambda: cparser.RedisParser(InvalidResponse, ResponseError)
except ImportError:     # pragma    nocover
    HAS_C_EXTENSIONS = False
    CRedisParser = RedisParser

NOT_DONE = object()


class Request(pulsar.Request):
    '''Asynchronous Request for redis.'''
    def __init__(self, client, command_name, args, options=None,
                 raise_on_error=True, release_connection=True,
                 **inp_params):
        pool = client.connection_pool
        self.client = client
        self.parser = pool.parser()
        self.command_name = command_name.upper()
        self.raise_on_error = raise_on_error
        self.release_connection = release_connection
        self.args = args
        self.inp_params = inp_params
        if not command_name:
            self.response = []
            self.command = self.parser.pack_pipeline(args)
            self.args_options = deque(args)
        else:
            self.command = self.parser.pack_command(self.command_name, *args)
            self.options = options

    @property
    def key(self):
        return self.client.connection_info

    @property
    def address(self):
        return self.client.connection_info.address

    @property
    def timeout(self):
        return self.client.connection_info.timeout

    @property
    def connection_pool(self):
        return self.client.connection_pool

    @property
    def is_pipeline(self):
        return not bool(self.command_name)

    def __repr__(self):
        if self.is_pipeline:
            return 'PIPELINE%s' % self.args
        else:
            return '%s%s' % (self.command_name, self.args)
    __str__ = __repr__

    def feed(self, data):
        self.parser.feed(data)
        response = self.parser.get()
        client = self.client
        parse = client.parse_response
        if self.command_name:
            result = NOT_DONE
            while response is not False:
                result = response
                if not isinstance(response, Exception):
                    if self.options:
                        result = parse(result, self.command_name,
                                       **self.options)
                    else:
                        result = parse(result, self.command_name)
                elif self.raise_on_error:
                    raise response
                response = self.parser.get()
            return result
        else:
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


class RedisProtocol(pulsar.ProtocolConsumer):
    '''An asynchronous pulsar protocol for redis.'''
    result = NOT_DONE

    def data_received(self, data):
        self.result = self._request.feed(data)
        if self.result is not NOT_DONE:
            self.finished()

    def start_request(self):
        self.transport.write(self._request.command)


class Redis(redis.StrictRedis):
    '''Override redis-py client handler'''
    def __init__(self, poll, connection_info, full_response=False, **kw):
        self.connection_pool = poll
        self.connection_info = connection_info
        self.full_response = full_response
        self.extra = kw
        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

    @property
    def encoding(self):
        return self.connection_pool.encoding

    def execute_command(self, command, *args, **options):
        "Execute a ``command`` and return a parsed response"
        try:
            return self.connection_pool.request(self, command, args, options)
        except NoScriptError:
            self.connection_pool.clear_scripts()
            raise

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(self, self.response_callbacks, transaction, shard_hint)

    def pubsub(self, shard_hint=None):
        '''Return a Publish/Subscribe object.

        With this object, you can subscribe to channels and listen for
        messages that get published to them.
        '''
        return PubSub(self.connection_pool, self.connection_info, shard_hint,
                      self.extra)

    def register_script(self, script):
        raise NotImplementedError

    def parse_response(self, response, command_name, **options):
        "Parses a response from the Redis server."
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response


class BasePipeline(_BasePipeline):
    '''Asynchronous equivalent of the BasePipeline.
    '''
    def __init__(self, client, response_callbacks, transaction, shard_hint):
        self.client = client
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.connection = None
        self.reset()

    @property
    def connection_pool(self):
        return self.client.connection_pool

    @property
    def connection_info(self):
        return self.client.connection_info

    @property
    def full_response(self):
        return self.client.full_response

    def execute(self, raise_on_error=True):
        return self.connection_pool.request_pipeline(
            self, raise_on_error=raise_on_error)

    def parse_response(self, response, command_name, **options):
        if self.transaction:
            if command_name != 'EXEC':
                return response
            else:
                data = []
                callbacks = self.response_callbacks
                for r, cmd in zip(response, self.command_stack):
                    if not isinstance(r, Exception):
                        args, opt = cmd
                        command_name = args[0]
                        if command_name in callbacks:
                            r = callbacks[command_name](r, **opt)
                    data.append(r)
                return data
        else:
            callbacks = self.response_callbacks
            if command_name in callbacks:
                response = callbacks[command_name](response, **options)
            if command_name in self.UNWATCH_COMMANDS:
                self.watching = False
            elif command_name == 'WATCH':
                self.watching = True
            return response


class Pipeline(BasePipeline, Redis):
    pass


class PubSub(pulsar.ProtocolConsumer):
    '''Asynchronous Publish/Subscriber handler for redis.

    To listen for messages you can bind to the ``on_message`` event::

        from stdnet import getdb

        def handle_messages(channel_message):
            ...

        redis = getdb('redis://122.0.0.1:6379?timeout=0').client
        pubsub = redis.pubsub()
        pubsub.bind_event('on_message', handle_messages)
        pubsub.subscribe('mychannel')

    You can bind as many handlers to the ``on_message`` event as you like.
    The handlers receive one parameter only, a two-elements tuple
    containing the ``channel`` and the ``message``.
    '''
    parser = None
    MANY_TIMES_EVENTS = ('data_received', 'data_processed', 'on_message')
    subscribe_commands = frozenset((b'unsubscribe', b'punsubscribe',
                                    b'subscribe', b'psubscribe'))

    def __init__(self, connection_pool, connection_info, shard_hint, extra):
        super(PubSub, self).__init__()
        self.connection_pool = connection_pool
        self.connection_info = connection_info
        self.shard_hint = shard_hint
        self.extra = extra
        self._reset()
        self.bind_event('post_request', self._reset)

    @property
    def channels(self):
        '''The set of channels this handler is subscribed to.'''
        return frozenset(self._channels)

    @property
    def patterns(self):
        '''The set of patterns this handler is subscribed to.'''
        return frozenset(self._patterns)

    @property
    def is_pipeline(self):
        return False

    def client(self, full_response=True):
        return Redis(self.connection_pool, self.connection_info,
                     full_response=full_response)

    def publish(self, channel, message):
        '''Publish a new ``message`` to a ``channel``.

        This method return a pulsar Deferred which results in the number of
        subscribers that will receive the message (the same behaviour as
        redis publish command).
        '''
        return self.client(False).execute_command('PUBLISH', channel, message)

    def subscribe(self, *channels):
        '''Subscribe to a list of ``channels`` or ``channel patterns``.

        It returns an asynchronous component which results in the number of
        channels this handler is subscribed to.

        If this is the first time the method is called by this handler,
        than the :class:`PubSub` starts listening for messages which
        are fired via the ``on_message`` event.
        '''
        d = Deferred()
        loop = self.connection_pool.get_event_loop()
        loop.call_soon_threadsafe(self._subscribe, channels, d)
        return d

    def unsubscribe(self, *channels):
        '''Un-subscribe from a list of ``channels`` or ``channel patterns``.

        It returns an asynchronous component which results in the number of
        channels this handler is subscribed to.
        '''
        d = Deferred()
        loop = self.connection_pool.get_event_loop()
        loop.call_soon_threadsafe(self._unsubscribe, channels, d)
        return d

    def close(self):
        '''Stop listening for messages.

        :meth:`unsubscribe` from all :attr:`channels` and :attr:`patterns`
        and close the subscriber connection with redis.
        '''
        d = Deferred()
        loop = self.connection_pool.get_event_loop()
        loop.call_soon_threadsafe(self._close, d)
        return d

    def data_received(self, data):
        self.parser.feed(data)
        response = self.parser.get()
        while response is not False:
            if isinstance(response, list):
                command = response[0]
                if command == b'message':
                    response = response[1:3]
                    self.fire_event('on_message', response)
                elif command == b'pmessage':
                    response = response[2:4]
                    self.fire_event('on_message', response)
                elif command in self.subscribe_commands:
                    request = self._request
                    if request:
                        self._request = None
                        request.callback(response)
            else:
                raise ProtocolError
            response = self.parser.get()

    #    INTERNALS

    def _subscribe(self, channels, d):
        try:
            channels, patterns = self._channel_patterns(channels)
            if channels:
                channels = tuple(set(channels) - self._channels)
                if channels:
                    yield self._execute('subscribe', *channels)
                    self._channels.update(channels)
            if patterns:
                patterns = tuple(set(patterns) - self._patterns)
                if patterns:
                    yield self._execute('psubscribe', *patterns)
                    self._patterns.update(patterns)
            d.callback(self._count_channels())
        except Exception:
            d.callback(sys.exc_info())

    def _unsubscribe(self, channels, d):
        try:
            channels, patterns = self._channel_patterns(channels)
            if not channels and not patterns:
                if self._channels:
                    yield self._execute('unsubscribe')
                    self._channels = set()
                if self._patterns:
                    yield self._execute('punsubscribe')
                    self._patterns = set()
            else:
                channels = self._channels.intersection(channels)
                patterns = self._patterns.intersection(patterns)
                if channels:
                    yield self._execute('unsubscribe', *channels)
                    self._channels.difference_update(channels)
                if patterns:
                    yield self._execute('punsubscribe', *patterns)
                    self._patterns.difference_update(patterns)
            d.callback(self._count_channels())
        except Exception:
            d.callback(sys.exc_info())

    def _close(self, future):
        if self._connection:
            d = Deferred()
            exc_info = None
            try:
                yield self._unsubscribe((), d)
                yield d
            except Exception:
                exc_info = sys.exc_info()
            self._connection.close()
            self._reset()
            future.callback(exc_info)

    def _channel_patterns(self, channels):
        patterns = []
        simples = []
        for c in channels:
            if '*' in c:
                if c != '*':
                    patterns.append(c)
            else:
                simples.append(c)
        return simples, patterns

    def _count_channels(self):
        return len(self._channels) + len(self._patterns)

    def _execute(self, command, *args):
        if not self._connection:
            client = self.client()
            pool = client.connection_pool
            consumer = yield pool.request(client, 'ping', (),
                                          release_connection=False).on_finished
            self.parser = consumer._request.parser
            connection = consumer.connection
            connection.set_consumer(None)
            connection.set_consumer(self)
            self.fire_event('pre_request')
        d = Deferred()
        yield self._connection_msg(command, args, d)

    def _connection_msg(self, command, args, d):
        if not self._request:
            self._request = d
            request = Request(self, command, args, release_connection=False)
            self.transport.write(request.command)
        else:
            self.event_loop.call_soon(self._connection_msg, command, args, d)
        return d

    def _reset(self, connection=None):
        if self._request:
            self._request.canel()
        self._connection = None
        self._request = None
        self._channels = set()
        self._patterns = set()
        return connection
