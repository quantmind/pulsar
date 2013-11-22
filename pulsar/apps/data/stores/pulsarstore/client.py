from itertools import chain
from hashlib import sha1

import pulsar
from pulsar.utils.structures import mapping_iterator
from pulsar.utils.pep import native_str, zip

from .pubsub import PubSub
from ...server import COMMANDS_INFO


INVERSE_COMMANDS_INFO = dict(((i.method_name, i.name)
                              for i in COMMANDS_INFO.values()))


class CommandError(pulsar.PulsarException):
    pass


class Executor:
    __slots__ = ('client', 'command')

    def __init__(self, client, command):
        self.client = client
        self.command = command

    def __call__(self, *args, **options):
        return self.client.execute(self.command, *args, **options)


def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged


def pairs_to_object(response, factory=None):
    it = iter(response)
    return (factory or dict)(zip(it, it))


def values_to_object(response, fields=None, factory=None):
    if fields is not None:
        return (factory or dict)(zip(fields, response))
    else:
        return response


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
         'HGETALL': pairs_to_object,
         'HMGET': values_to_object
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


class Client(object):
    '''Client for pulsar and redis stores.
    '''
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

    def hmget(self, key, *fields):
        return self.execute('hmget', key, *fields, fields=fields)

    def hmset(self, key, iterable):
        args = []
        [args.extend(pair) for pair in mapping_iterator(iterable)]
        return self.execute('hmset', key, *args)

    def eval(self, script, keys=None, args=None):
        return self._eval('eval', script, keys, args)

    def evalsha(self, sha, keys=None, args=None):
        return self._eval('evalsha', sha, keys, args)

    def __getattr__(self, name):
        command = INVERSE_COMMANDS_INFO.get(name)
        if command:
            return Executor(self, command)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (type(self), name))

    def _eval(self, command, script, keys, args):
        all = keys if keys is not None else ()
        num_keys = len(all)
        if args:
            all = tuple(chain(all, args))
        return self.execute(command, script, num_keys, *all)


class Pipeline(Client):

    def __init__(self, store):
        self.store = store
        self.reset()

    def execute(self, *args, **kwargs):
        self.command_stack.append((args, kwargs))
    execute_command = execute

    def reset(self):
        self.command_stack = []

    def commit(self, raise_on_error=True):
        cmds = list(chain([(('multi',), {})],
                          self.command_stack, [(('exec',), {})]))
        self.reset()
        return self.store.execute_pipeline(cmds, raise_on_error)


class RedisScriptMeta(type):

    def __new__(cls, name, bases, attrs):
        super_new = super(RedisScriptMeta, cls).__new__
        abstract = attrs.pop('abstract', False)
        new_class = super_new(cls, name, bases, attrs)
        if not abstract:
            self = new_class(new_class.script, new_class.__name__)
            _scripts[self.name] = self
        return new_class


class RedisScript(RedisScriptMeta('_RS', (object,), {'abstract': True})):
    '''Class which helps the sending and receiving lua scripts.

    It uses the ``evalsha`` command.

    .. attribute:: script

        The lua script to run

    .. attribute:: required_scripts

        A list/tuple of other :class:`RedisScript` names required by this
        script to properly execute.

    .. attribute:: sha1

        The SHA-1_ hexadecimal representation of :attr:`script` required by the
        ``EVALSHA`` redis command. This attribute is evaluated by the library,
        it is not set by the user.

    .. _SHA-1: http://en.wikipedia.org/wiki/SHA-1
    '''
    abstract = True
    script = None
    required_scripts = ()

    def __init__(self, script, name):
        if isinstance(script, (list, tuple)):
            script = '\n'.join(script)
        self.__name = name
        self.script = script
        rs = set((name,))
        rs.update(self.required_scripts)
        self.required_scripts = rs

    @property
    def name(self):
        return self.__name

    @property
    def sha1(self):
        if not hasattr(self, '_sha1'):
            self._sha1 = sha1(self.script.encode('utf-8')).hexdigest()
        return self._sha1

    def __repr__(self):
        return self.name if self.name else self.__class__.__name__
    __str__ = __repr__

    def preprocess_args(self, client, args):
        return args

    def callback(self, response, **options):
        '''Called back after script execution.

        This is the only method user should override when writing a new
        :class:`RedisScript`. By default it returns ``response``.

        :parameter response: the response obtained from the script execution.
        :parameter options: Additional options for the callback.
        '''
        return response

    def __call__(self, client, keys, args, options):
        args = self.preprocess_args(client, args)
        numkeys = len(keys)
        keys_args = tuple(keys) + args
        options.update({'script': self, 'redis_client': client})
        return client.execute_command('EVALSHA', self.sha1, numkeys,
                                      *keys_args, **options)
