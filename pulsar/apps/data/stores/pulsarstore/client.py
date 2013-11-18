from itertools import chain
from hashlib import sha1

import pulsar
from pulsar.utils.structures import mapping_iterator

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

    def __call__(self, *args):
        return self.client.execute(self.command, *args)


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

    def hmset(self, key, iterable):
        args = []
        [args.extend(pair) for pair in mapping_iterator(iterable)]
        return self.execute('hmset', key, *args)

    def __getattr__(self, name):
        command = INVERSE_COMMANDS_INFO.get(name)
        if command:
            return Executor(self, command)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (type(self), name))


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
        cmds = list(chain([(('multi', ), {})],
                          self.command_stack, [(('exec', ), {})]))
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
