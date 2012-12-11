import time
from multiprocessing import Process, current_process
from threading import Thread, current_thread

from pulsar import system, wrap_socket, platform, socket_pair
from pulsar.utils.security import gen_unique_id

from .iostream import AsyncIOStream
from .proxy import ActorProxyMonitor, get_proxy
from .defer import pickle, EXIT_EXCEPTIONS
from .access import get_actor, get_actor_from_id


__all__ = ['Concurrency', 'concurrency']

def concurrency(kind, actor_class, monitor, commands_set, cfg, **params):
    '''Function invoked by the :class:`Arbiter` or a :class:`Monitor` when
spawning a new :class:`Actor`. It created a :class:`Concurrency` instance
which handle the contruction and the lif of an :class:`Actor`.

:paramater kind: Type of concurrency
:paramater monitor: The monitor (or arbiter) managing the :class:`Actor`.
:rtype: a :class:`Councurrency` instance
'''
    if kind == 'monitor':
        c = MonitorConcurrency()
    elif kind == 'thread':
        c = ActorThread()
    elif kind == 'process':
        c = ActorProcess()
    else:
        raise ValueError('Concurrency %s not supported in pulsar' % kind)
    return c.make(kind, actor_class, monitor, commands_set, cfg, **params)


class Concurrency(object):
    '''Actor implementation is responsible for the actual spawning of
actors according to a concurrency implementation. Instances are pickable
and are shared between the :class:`Actor` and its
:class:`ActorProxyMonitor`.

:parameter concurrency: string indicating the concurrency implementation.
    Valid choices are ``monitor``, ``process`` and ``thread``.
:parameter actor_class: :class:`Actor` or one of its subclasses.
:parameter timeout: timeout in seconds for the actor.
:parameter kwargs: additional key-valued arguments to be passed to the actor
    constructor.
'''
    _creation_counter = 0
    address = None
    def make(self, kind, actor_class, monitor, commands_set, cfg, name=None,
             aid=None, **params):
        self.__class__._creation_counter += 1
        self.aid = aid or gen_unique_id()[:8]
        self.age = self.__class__._creation_counter
        self.name = name or actor_class.__name__.lower()
        self.kind = kind
        self.commands_set = commands_set
        self.cfg = cfg
        self.actor_class = actor_class
        self.params = params
        return self.get_actor(monitor)

    @property
    def unique_name(self):
        return '%s(%s)' % (self.name, self.aid)

    def __repr__(self):
        return self.unique_name
    __str__ = __repr__

    def get_actor(self, monitor):
        self.daemon = True
        if monitor.is_arbiter():
            arbiter = monitor
        else:
            arbiter = monitor.arbiter
        self.params['arbiter'] = get_proxy(arbiter)
        self.params['monitor'] = get_proxy(monitor)
        return ActorProxyMonitor(self)
                


class MonitorConcurrency(Concurrency):
    '''An actor implementation for Monitors. Monitors live in the main process
loop and therefore do not require an inbox.'''
    def get_actor(self, arbiter):
        self.params['arbiter'] = arbiter
        return self.actor_class(self)

    def start(self):
        pass

    def is_active(self):
        return self.actor.is_alive()

    @property
    def pid(self):
        return current_process().pid


class ActorConcurrency(Concurrency):

    def run(self):
        try:
            actor = self.actor_class(self)
            actor.start()
        except EXIT_EXCEPTIONS:
            # This is needed in windows in order to avoid useless traceback
            # on KeyboardInterrupt
            pass


class ActorProcess(ActorConcurrency, Process):
    '''Actor on a process'''
    pass


class ActorThread(ActorConcurrency, Thread):
    '''Actor on a thread'''
    def terminate(self):
        '''Called by the main thread to force termination.'''
        actor = get_actor_from_id(self.aid)
        if actor is not None:
            actor.exit()

    @property
    def pid(self):
        return current_process().pid

