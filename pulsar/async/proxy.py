from pulsar import CommandNotFound
from pulsar.utils.pep import default_timer

from .futures import Future, chain_future
from .consts import *   # noqa

__all__ = ['ActorProxy',
           'ActorProxyMonitor',
           'get_proxy',
           'command',
           'get_command']

global_commands_table = {}


def get_proxy(obj, safe=False):
    if isinstance(obj, ActorProxy):
        return obj
    elif hasattr(obj, 'proxy'):
        return get_proxy(obj.proxy)
    else:
        if safe:
            return None
        else:
            raise ValueError('"%s" is not an actor or actor proxy.' % obj)


def actor_identity(actor):
    return actor.aid if hasattr(actor, 'aid') else actor


def get_command(name):
    '''Get the command function *name*'''
    command = global_commands_table.get(name.lower())
    if not command:
        raise CommandNotFound(name)
    return command


class command:
    '''Decorator for pulsar command functions.

    :parameter ack: ``True`` if the command acknowledge the sender with a
        response. Usually is set to ``True`` (which is also the default value).
    '''
    def __init__(self, ack=True):
        self.ack = ack

    def __call__(self, f):
        self.name = f.__name__.lower()

        def command_function(request, args, kwargs):
            return f(request, *args, **kwargs)

        command_function.ack = self.ack
        command_function.__name__ = self.name
        command_function.__doc__ = f.__doc__
        global_commands_table[self.name] = command_function
        return command_function


def actor_proxy_future(aid, future=None):
    self = Future()
    if isinstance(aid, ActorProxyMonitor):
        assert future is None
        aid.callback = self
        self.aid = aid.aid
    else:
        self.aid = aid
        chain_future(future, next=self)
    return self


class ActorProxy:
    '''A proxy for a remote :class:`.Actor`.

    This is a lightweight class which delegates function calls to the
    underlying remote object.

    It is picklable and therefore can be send from actor to actor using
    :ref:`actor message passing <tutorials-messages>`.

    For example, lets say we have a proxy ``a``, to send a message to it::

        from pulsar import send

        send(a, 'echo', 'hello there!')

    will send the :ref:`command <actor_commands>` ``echo`` to actor ``a`` with
    parameter ``"hello there!"``.

    .. attribute:: aid

        Unique ID for the remote :class:`.Actor`

    .. attribute:: address

        the socket address of the underlying :attr:`.Actor.mailbox`.

    '''
    def __init__(self, impl):
        self.aid = impl.aid
        self.name = impl.name
        self.cfg = impl.cfg
        self.address = getattr(impl, 'address', None)

    def __repr__(self):
        return '%s(%s)' % (self.name, self.aid)
    __str__ = __repr__

    @property
    def proxy(self):
        return self

    def __eq__(self, o):
        o = get_proxy(o, True)
        return o and self.aid == o.aid

    def __ne__(self, o):
        return not self.__eq__(o)


class ActorProxyMonitor(ActorProxy):
    '''A specialised :class:`.ActorProxy` class.

    It contains additional information about the remote underlying
    :class:`.Actor`. Instances of this class serialise into
    :class:`.ActorProxy`.

    The :class:`.ActorProxyMonitor` is special since it lives in the
    :class:`.Arbiter` domain and it is used by the :class:`.Arbiter`
    (or a :class:`.Monitor`) to monitor the state of the spawned actor.

    .. attribute:: impl

        The :class:`.Concurrency` instance for the remote :class:`.Actor`. This
        dictionary is constantly updated by the remote actor by sending the
        :ref:`info message <actor_info_command>`.

    .. attribute:: info

        Dictionary of information regarding the remote :class:`.Actor`

    .. attribute:: mailbox

        This is the connection with the remote actor. It is available once the
        :ref:`actor handshake <handshake>` between the actor and the monitor
        has completed. The :attr:`mailbox` is a server-side
        :class:`.MailboxProtocol` instance and it is used
        by the :func:`.send` function to send messages to the remote actor.
    '''
    monitor = None

    def __init__(self, impl):
        self.impl = impl
        self.info = {}
        self.mailbox = None
        self.callback = None
        self.spawning_start = None
        self.stopping_start = None
        super().__init__(impl)

    @property
    def notified(self):
        '''Last time this :class:`.ActorProxyMonitor` was notified by the
        remote actor.'''
        return self.info.get('last_notified')

    @property
    def proxy(self):
        '''The :class:`.ActorProxy` for this monitor.'''
        return ActorProxy(self)

    def __reduce__(self):
        return self.proxy.__reduce__()

    def is_alive(self):
        '''``True`` if underlying actor is alive.
        '''
        return self.impl.is_alive()

    def terminate(self):
        '''Terminate life of underlying actor.
        '''
        self.impl.terminate()

    def join(self, timeout=None):
        '''Wait until the underlying actor terminates.

        If ``timeout`` is provided, it raises an exception if the timeout
        is reached.
        '''
        self.impl.join(timeout=timeout)

    def start(self):
        '''Start the remote actor.
        '''
        self.spawning_start = default_timer()
        self.impl.start()

    def should_be_alive(self):
        if not self.mailbox:
            return default_timer() - self.spawning_start > ACTOR_ACTION_TIMEOUT
        else:
            return True

    def should_terminate(self):
        if self.stopping_start is None:
            self.stopping_start = default_timer()
            return False
        else:
            dt = default_timer() - self.stopping_start
            return dt if dt >= ACTOR_ACTION_TIMEOUT else False
