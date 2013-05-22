import os
import sys
from time import time

import pulsar
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.pep import iteritems, itervalues, range

from . import proxy
from .actor import Actor, send
from .defer import async, NOT_DONE
from .concurrency import concurrency
from .mailbox import MonitorMailbox
from .consts import *


__all__ = ['Monitor', 'PoolMixin']


def _spawn_actor(cls, monitor, cfg=None, name=None, aid=None, **kw):
    # Internal function which spawns a new Actor and return its
    # ActorProxyMonitor.
    # *cls* is the Actor class
    # *monitor* can be either the ariber or a monitor
    kind = None
    if issubclass(cls, PoolMixin):
        kind = 'monitor'
    if cfg is None:
        if monitor:
            cfg = monitor.cfg.copy()
        else:
            cfg = pulsar.Config()
    if monitor:
        params = monitor.actorparams()
        name = params.pop('name', name)
        aid = params.pop('aid', aid)
    else:
        if kind != 'monitor':
            raise TypeError('class %s not a valid monitor' % cls)
        params = {}
    for key, value in iteritems(kw):
        if key in cfg.settings:
            cfg.set(key, value)
        else:
            params[key] = value
    #
    if monitor:
        if not kind:
            if not issubclass(cls, Actor):
                raise TypeError('Class %s not a valid actor.' % cls)
            kind = cfg.concurrency
    if not kind:
        raise TypeError('Cannot spawn class %s. not a valid concurrency.' % cls)
    actor_proxy = concurrency(kind, cls, monitor, cfg, name=name,
                              aid=aid, **params)
    # Add to the list of managed actors if this is a remote actor
    if isinstance(actor_proxy, Actor):
        return actor_proxy
    else:
        actor_proxy.monitor = monitor
        monitor.managed_actors[actor_proxy.aid] = actor_proxy
        deferred = proxy.ActorProxyDeferred(actor_proxy)
        actor_proxy.start()
        return deferred


class PoolMixin(Actor):
    '''Not an actor per se, this is a mixin for :class:`Actor`
which manages a pool (group) of actors. Given an :attr:`actor_class`
it makes sure there are always :attr:`cfg.workers` alive.
It is used by both the :class:`Arbiter` and the :class:`Monitor` classes.

.. attribute:: managed_actors

    dictionary with keys given by actor's ids and values by
    :class:`ActorProxyMonitor` instances. These are the actors managed by the
    pool.

'''
    CLOSE_TIMEOUT = 30000000000000
    actor_class = Actor
    '''The class derived form :class:`Actor` which the monitor manages
during its life time.

    Default: :class:`Actor`'''

    def __init__(self, impl):
        super(PoolMixin, self).__init__(impl)
        self.managed_actors = {}
        self.terminated_actors = []
        self.actor_class = self.params.pop('actor_class') or self.actor_class

    def hand_shake(self, *args):
        #Monitors don't do hand shakes
        try:
            self.state = ACTOR_STATES.RUN
            self.fire_event('start')
            self.periodic_task()
        except Exception:
            self.stop(sys.exc_info())
    
    def get_actor(self, aid):
        aid = getattr(aid, 'aid', aid)
        if aid == self.aid:
            return self
        elif aid in self.managed_actors:
            return self.managed_actors[aid]
        elif self.monitor and aid==self.monitor.aid:
            return self.monitor
        
    def active(self):
        return self.running()

    def spawn(self, actor_class=None, **params):
        '''Spawn a new :class:`Actor` and return its
:class:`ActorProxyMonitor`.'''
        actor_class = actor_class or self.actor_class
        return _spawn_actor(actor_class, self, **params)

    def actorparams(self):
        '''Return a dictionary of parameters to be passed to the
spawn method when creating new actors.'''
        return dict(self.params)

    def _remove_actor(self, actor, log=True):
        if log:
            self.logger.info('Removing %s', actor)
        self.managed_actors.pop(actor.aid, None)
        if self.monitor:
            self.monitor._remove_actor(actor, False)
                
    def manage_actors(self, stop=False):
        '''Remove :class:`Actor` which are not alive from the
:class:`PoolMixin.managed_actors` and return the number of actors still alive.

:parameter stop: if ``True`` stops all alive actor.
'''
        alive = 0
        if self.managed_actors:
            for aid, actor in list(iteritems(self.managed_actors)):
                alive += self.manage_actor(actor, stop)
        return alive

    def manage_actor(self, actor, stop=False):
        '''If an actor failed to notify itself to the arbiter for more than
the timeout. Stop the arbiter.'''
        if not self.running():
            stop = True
        if not actor.is_alive():
            if not actor.should_be_alive() and not stop:
                return 1
            actor.join(ACTOR_TERMINATE_TIMEOUT)
            self._remove_actor(actor)
            return 0
        timeout = None
        started_stopping = bool(actor.stopping_start)
        stop = stop or started_stopping
        if not stop and actor.notified:
            gap = time() - actor.notified
            stop = timeout = gap > actor.cfg.timeout
        if stop:   # we are shutting down
            if not actor.mailbox or actor.should_terminate():
                if not actor.mailbox:
                    self.logger.info('Terminating %s. No mailbox.', actor)
                else:
                    self.logger.warning('Terminating %s. Timeout.', actor)
                actor.terminate()
                self.terminated_actors.append(actor)
                self._remove_actor(actor)
                return 0
            elif not started_stopping:
                if timeout:
                    self.logger.warning('Stopping %s. Timeout', actor)
                else:
                    self.logger.info('Stopping %s.', actor)
                self.send(actor, 'stop')
        return 1

    def spawn_actors(self):
        '''Spawn new actors if needed. If the :class:`PoolMixin` is spawning
do nothing.'''
        to_spawn = self.cfg.workers - len(self.managed_actors)
        if self.cfg.workers and to_spawn > 0:
            for _ in range(to_spawn):
                self.spawn()

    def stop_actors(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if self.cfg.workers:
            num_to_kill = len(self.managed_actors) - self.cfg.workers
            for i in range(num_to_kill, 0, -1):
                w, kage = 0, sys.maxsize
                for worker in itervalues(self.managed_actors):
                    age = worker.impl.age
                    if age < kage:
                        w, kage = w, age
                self.manage_actor(w, True)
    
    @async()
    def close_actors(self):
        '''Close all managed :class:`Actor`.'''
        # Stop all of them
        to_stop = self.manage_actors(stop=True)
        while to_stop:
            yield NOT_DONE
            to_stop = self.manage_actors(stop=True)
    
    
class Monitor(PoolMixin):
    '''A monitor is a **very** special :class:`Actor` and :class:`PoolMixin`
which shares the same :class:`EventLoop` with the :class:`Arbiter` and
therefore lives in the main thread of the  process domain.
The Arbiter manages monitors which in turn manage a set of :class:`Actor`
performing similar tasks.

In other words, you may have a monitor managing actors for serving HTTP
requests on a given port, another monitor managing actors consuming tasks
from a task queue and so forth. You can think of :class:`Monitor` as
managers of pools of :class:`Actor`.

Monitors are created by invoking the :meth:`Arbiter.add_monitor`
functions and not by directly invoking the constructor. Therefore
adding a new monitor to the arbiter follows the pattern::

    import pulsar

    m = pulsar.arbiter().add_monitor(pulsar.Monitor, 'mymonitor')
'''
    socket = None

    @property
    def cpubound(self):
        return False
    
    @property
    def arbiter(self):
        return self.monitor

    def is_process(self):
        return False

    def is_monitor(self):
        return True

    def monitor_task(self):
        '''Monitor specific task called by the :meth:`Monitor.periodic_task`.
By default it does nothing. Override if you need to.'''
        pass

    def periodic_task(self):
        '''Overrides the :meth:`Actor.on_task`
:ref:`actor callback <actor-callbacks>` to perform
the monitor :class:`EventLoop` tasks, which are:

* To maintain a responsive set of actors ready to perform their duty.
* To perform its own tasks.

The implementation goes as following:

* It calls :meth:`PoolMixin.manage_actors` which removes from the live
  actors dictionary all actors which are not alive.
* Spawn new actors if required by calling :meth:`PoolMixin.spawn_actors`
  and :meth:`PoolMixin.stop_actors`.
* Call :meth:`Monitor.monitor_task` which performs the monitor specific
  task.

Users shouldn't need to override this method, but use
:meth:`Monitor.monitor_task` instead.'''
        if self.running():
            self.manage_actors()
            self.spawn_actors()
            self.stop_actors()
            self.monitor_task()
        self.event_loop.call_soon(self.periodic_task)

    # HOOKS
    @async(max_errors=None)
    def _stop(self):
        yield self.close_actors()
        if not self.terminated_actors:
            self.monitor._remove_actor(self)
        self.fire_event('stop')

    @property
    def multithread(self):
        return self.cfg.concurrency == 'thread'

    @property
    def multiprocess(self):
        return self.cfg.concurrency == 'process'
    
    @property
    def requestloop(self):
        return self.monitor.requestloop

    def info(self):
        data = {'actor': {'actor_class':self.actor_class.__name__,
                          'concurrency':self.cfg.concurrency,
                          'name':self.name,
                          'age':self.impl.age,
                          'workers': len(self.managed_actors)}}
        if not self.started():
            return data
        data['workers'] = [a.info for a in itervalues(self.managed_actors)\
                           if a.info]
        return data

    def proxy_mailbox(address):
        return self.arbiter.proxy_mailboxes.get(address)

    def get_actor(self, aid):
        #Delegate get_actor to the arbiter
        a = super(Monitor, self).get_actor(aid)
        if a is None:
            a = self.monitor.get_actor(aid)
        return a

    # OVERRIDES INTERNALS        
    def _setup_event_loop(self):
        self.mailbox = self._mailbox()
        
    def _run(self):
        pass
    
    def _mailbox(self):
        return MonitorMailbox(self)
