import time
from multiprocessing import Process, current_process
from threading import Thread, current_thread

from pulsar import system, platform
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import pickle

from .proxy import ActorProxyMonitor, get_proxy
from .access import get_actor, get_actor_from_id
from .threads import KillableThread


__all__ = ['Concurrency', 'concurrency']

def concurrency(kind, actor_class, monitor, cfg, **params):
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
    return c.make(kind, actor_class, monitor, cfg, **params)


class Concurrency(object):
    '''Actor :class:`Concurrency` is responsible for the actual spawning of
actors according to a concurrency implementation. Instances are pickable
and are shared between the :class:`Actor` and its
:class:`ActorProxyMonitor`.

:parameter concurrency: string indicating the concurrency implementation.
    Valid choices are ``monitor``, ``process``, ``thread``.
:parameter actor_class: :class:`Actor` or one of its subclasses.
:parameter timeout: timeout in seconds for the actor.
:parameter kwargs: additional key-valued arguments to be passed to the actor
    constructor.
'''
    _creation_counter = 0
    def make(self, kind, actor_class, monitor, cfg, name=None, aid=None, **kw):
        self.__class__._creation_counter += 1
        self.aid = aid or gen_unique_id()[:8]
        self.age = self.__class__._creation_counter
        self.name = name or actor_class.__name__.lower()
        self.kind = kind
        self.cfg = cfg
        self.actor_class = actor_class
        self.params = kw
        self.params['monitor'] = monitor
        return self.get_actor()

    @property
    def unique_name(self):
        return '%s(%s)' % (self.name, self.aid)

    def __repr__(self):
        return self.unique_name
    __str__ = __repr__

    def get_actor(self):
        self.daemon = True
        self.params['monitor'] = get_proxy(self.params['monitor'])
        # make sure these parameters are pickable
        #pickle.dumps(self.params)
        return ActorProxyMonitor(self)


class MonitorConcurrency(Concurrency):
    ''':class:`Concurrency` class for monitors such as the :class:`Arbiter`
and :class:`Monitor`. Monitors live in the **mainthread** of the master process
and therefore do not require to be spawned.'''
    def get_actor(self):
        return self.actor_class(self)

    def start(self):
        pass

    def is_active(self):
        return self.actor.is_alive()

    @property
    def pid(self):
        return current_process().pid


class ActorConcurrency(Concurrency):
    '''Base class for all :class:`Actor` concurrency models. Must implement
the **start** method.'''
    def run(self):
        actor = self.actor_class(self)
        actor.start()


class ActorProcess(ActorConcurrency, Process):
    '''Actor on a process'''


class ActorThread(ActorConcurrency, KillableThread):
    '''Actor on a thread'''

    def terminate(self, kill=False):
        if self.is_alive():
            actor = get_actor_from_id(self.aid)
            me = get_actor()
            if not kill and actor and actor != me:
                actor.stop(1)
                me.event_loop.call_later(1, self.terminate, True)
            else:
                KillableThread.terminate(self)
