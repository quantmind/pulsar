import time
from multiprocessing import Process, current_process
from threading import Thread, current_thread

from pulsar import system, wrap_socket, platform, socket_pair
from pulsar.utils.security import gen_unique_id

from .iostream import AsyncIOStream
from .proxy import ActorProxyMonitor, get_proxy
from .defer import pickle
from .access import get_actor, get_actor_from_id


__all__ = ['Concurrency', 'concurrency']


def concurrency(kind, actor_class, timeout, monitor, aid, commands_set, params):
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
        raise ValueError('Concurrency %s not supported by pulsar' % kind)
    return c.make(kind, actor_class, timeout, monitor,
                  aid, commands_set, params)


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
    def make(self, kind, actor_class, timeout, monitor, aid,
             commands_set, kwargs):
        if not aid:
            if monitor and monitor.is_arbiter():
                aid = 'arbiter'
            else:
                aid = gen_unique_id()[:8]
        self.aid = aid
        self.impl = kind
        self.commands_set = commands_set
        self.timeout = timeout
        self.actor_class = actor_class
        self.loglevel = kwargs.pop('loglevel',None)
        self.a_kwargs = kwargs
        return self.get_actor(monitor)

    @property
    def name(self):
        return '{0}({1})'.format(self.actor_class.code(), self.aid)

    def __str__(self):
        return self.name

    def get_actor(self, monitor):
        self.daemon = True
        if monitor.is_arbiter():
            arbiter = monitor
        else:
            arbiter = monitor.arbiter
        self.a_kwargs['arbiter'] = get_proxy(arbiter)
        self.a_kwargs['monitor'] = get_proxy(monitor)
        return ActorProxyMonitor(self)


class MonitorConcurrency(Concurrency):
    '''An actor implementation for Monitors. Monitors live in the main process
loop and therefore do not require an inbox.'''
    def get_actor(self, arbiter):
        # Override get_actor
        self.a_kwargs['arbiter'] = arbiter
        self.timeout = 0
        return self.actor_class(self, **self.a_kwargs)

    def start(self):
        pass

    def is_active(self):
        return self.actor.is_alive()

    @property
    def pid(self):
        return current_process().pid


class ActorConcurrency(Concurrency):

    def run(self):
        actor = self.actor_class(self, **self.a_kwargs)
        actor.start()


class ActorProcess(ActorConcurrency, Process):
    '''Actor on a process'''
    pass


class ActorThread(ActorConcurrency, Thread):
    '''Actor on a thread'''
    def terminate(self):
        '''Called by the main thread to force termination.'''
        actor = get_actor_from_id(self.aid)
        result = actor.stop(force=True)

    @property
    def pid(self):
        return current_process().pid

