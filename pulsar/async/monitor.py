import os
import sys
import time

import pulsar
from pulsar.utils.py2py3 import iteritems, itervalues

from .actor import Actor
from .defer import make_async, Deferred, is_async
from .proxy import ActorCallBacks


__all__ = ['Monitor','PoolMixin']


class PoolMixin(object):
    '''Not an actor per se, this is a mixin for :class:`Actor`
which manages a pool (group) of actors. Given an :attr:`actor_class`
it makes sure there are always :attr:`num_actors` alive.

.. attribute:: MANAGED_ACTORS

    dictionary with keys given by actor's ids and values by
    :class:`ActorProxyMonitor` instances. These are the actors managed by the
    pool.
    
.. attribute:: num_actors

    Number of actors to manage.
    
    Default ``0`` any number of actors.
    
.. attribute:: actor_class

    The class derived form :class:`pulsar.Actor` which the monitor manages
    during its life time.
    
    Default: :class:`Actor`
'''
    def on_init(self, impl, actor_class = None, address = None,
                actor_params = None, num_actors = 0, **kwargs):
        self._managed_actors = {}
        self.num_actors = num_actors or 0
        self.actor_class = actor_class or Actor
        self.address = address
        self._actor_params = actor_params
        
    def get_actor(self, aid):
        a = Actor.get_actor(self, aid)
        if not a and aid in self.MANAGED_ACTORS:
            a = self.MANAGED_ACTORS[aid]
        return a
               
    @property
    def MANAGED_ACTORS(self):
        return self._managed_actors
    
    def manage_actors(self):
        """Remove actors not alive"""
        ACTORS = self.MANAGED_ACTORS
        linked = self._linked_actors
        for aid,actor in list(iteritems(ACTORS)):
            if not actor.is_alive():
                ACTORS.pop(aid)
                linked.pop(aid,None)
            else:
                self.on_manage_actor(actor)
    
    def spawn_actors(self):
        '''Spawn new actors if needed'''
        if self.num_actors:
            while len(self.MANAGED_ACTORS) < self.num_actors:
                self.spawn_actor()
    
    def stop_actors(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if self.num_actors:
            num_to_kill = len(self.MANAGED_ACTORS) - self.num_actors
            for i in range(num_to_kill, 0, -1):
                w, kage = 0, sys.maxsize
                for worker in iteritems(self.MANAGED_ACTORS):
                    age = worker.age
                    if age < kage:
                        w, kage = w, age
                self.stop_actor(w)
                
    def stop_actor(self, actor):
        raise NotImplementedError


class Monitor(PoolMixin,Actor):
    '''\
A monitor is a special :class:`pulsar.Actor` which shares
the same event loop with the :class:`pulsar.Arbiter`
and therefore lives in the main process. The Arbiter manages monitors which
in turn manage a set of :class:`pulsar.Actor` performing similar tasks.

Therefore you may
have a monitor managing actors for serving HTTP requests on a given port,
another monitor managing actors consuming tasks from a task queue and so forth.

Monitors are created by invoking the :meth:`pulsar.Arbiter.add_monitor`
functions and not by directly invoking the constructor. Therefore
adding a new monitor to the arbiter follows the pattern::

    import pulsar
    
    m = pulsar.arbiter().add_monitor(pulsar.Monitor,'mymonitor')
    
You can also create a monitor with a distributed queue as IO mechanism::

    from multiprocessing import Queue
    import pulsar
    
    m = pulsar.arbiter().add_monitor(pulsar.Monitor,
                                     'mymonitor',
                                     ioqueue = Queue())

    
'''
    socket = None
    
    def isprocess(self):
        return False
        
    def monitor_task(self):
        '''Monitor specific task called by the :meth:`pulsar.Monitor.on_task`
:ref:`actor callback <actor-callbacks>` at each iteration in the event loop.
By default it does nothing.'''
        pass
    
    # HOOKS        
    def on_task(self):
        '''Overrides the :meth:`pulsar.Actor.on_task`
:ref:`actor callback <actor-callbacks>` to perform
the monitor event loop tasks: a) maintain a responsive set of actors ready
to perform their duty and b) perform its own task.

The monitor performs its tasks in the following way:

* It calls :meth:`pulsar.Monitor.manage_actors` which removes from the live
  actors dictionary all actors which are not alive.
* Spawn new actors if required by calling :meth:`pulsar.Monitor.spawn_actors`
  and :meth:`pulsar.Monitor.stop_actors`.
* Call :meth:`pulsar.Monitor.monitor_task` which performs the monitor specific
  task.
  
  User should not override this method, and use
  :meth:`pulsar.Monitor.monitor_task` instead.'''
        self.manage_actors()
        if not self._stopping:
            self.spawn_actors()
            self.stop_actors()
            self.monitor_task()
        
    def on_stop(self):
        '''Ovverrides the :meth:`pulsar.Actor.on_stop` 
:ref:`actor callback <actor-callbacks>` by stopping
all actors managed by ``self``.'''
        self.log.debug('exiting "{0}"'.format(self))
        for actor in self.linked_actors():
            actor.stop()
        
    # OVERRIDES
    
    def init_runner(self):
        pass
    
    def _make_name(self):
        return 'Monitor-{0}({1})'.format(self.actor_class.code(),self.aid)
    
    def get_eventloop(self, impl):
        '''Return the arbiter event loop.'''
        return self.arbiter.ioloop
    
    def _stop_ioloop(self):
        return make_async()
    
    def _run(self):
        pass
    
    @property
    def multithread(self):
        return self.cfg.concurrency == 'thread'
    
    @property
    def multiprocess(self):
        return self.cfg.concurrency == 'process'
                
    def __join(self, timeout = 1):
        '''Join the pool, close or terminate must have been called before.'''
        if not self.stopped():
            raise ValueError('Cannot join worker pool. Must be stopped\
 or terminated first.')
        for wid, proxy in list(iteritems(self.WORKERS)):
            if not proxy.is_alive():
                self.clean_worker(wid)
            else:
                proxy.join(timeout)
            
    def spawn_actor(self):
        '''Spawn a new actor and add its :class:`pulsar.ActorProxyMonitor`
 to the :attr:`pulsar.Monitor.MANAGED_ACTORS` dictionary.'''
        worker = self.arbiter.spawn(
                        self.actor_class,
                        monitor = self,
                        ioqueue = self.ioqueue,
                        monitors = self.arbiter.get_all_monitors(),
                        **self.actorparams())
        monitor = self.arbiter.MANAGED_ACTORS[worker.aid]
        self.MANAGED_ACTORS[worker.aid] = monitor
        return worker
    
    def stop_actor(self, actor):
        if not actor.is_alive():
            self.MANAGED_ACTORS.pop(actor.aid)
        else:
            return actor.proxy.stop()
    
    def actorparams(self):
        '''Return a dictionary of parameters to be passed to the
spawn method when creating new actors.'''
        return self._actor_params or {}
        
    def info(self, full = False):
        if full:
            requests = []
            proxy = self.proxy
            for w in itervalues(self.MANAGED_ACTORS):
                requests.append(proxy.info(w))
            return ActorCallBacks(self,requests).add_callback(self._info)
        else:
            return self._info()
        
    def _info(self, result = None):
        if not result:
            result = [a.local_info() for a in self.MANAGED_ACTORS.values()] 
        tq = self.ioqueue
        return {'actor_class':self.actor_class.code(),
                'workers': result,
                'num_actors':len(self.MANAGED_ACTORS),
                'concurrency':self.cfg.concurrency,
                'listen':str(self.socket),
                'name':self.name,
                'age':self.age,
                'ioqueue': tq is not None,
                'ioqueue_size': tq.qsize() if tq else None}
        
    def get_actor(self, aid):
        '''Delegate get_actor to the arbiter'''
        return self.arbiter.get_actor(aid)
        
    