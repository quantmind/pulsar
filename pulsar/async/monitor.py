import os
import sys
import time

import pulsar
from pulsar.utils.py2py3 import iteritems, itervalues

from .actor import Actor
from .defer import make_async, Deferred, is_async
from .proxy import ActorCallBacks


__all__ = ['Monitor','ActorPool']


class ActorPool(Actor):
    '''An :class:`pulsar.Actor` which manages a pool (group) of actors.
This is the base class for :class:`pulsar.Arbiter` and :class:`pulsar.Monitor`.

.. attribute: num_workers

    Number of workers to manage. Default ``0`` any number of actors.
'''
    
    def _init(self, impl, *args, **kwargs):
        self._linked_actors = {}
        self.num_workers = kwargs.pop('num_workers',0) or 0
        super(ActorPool,self)._init(impl, *args, **kwargs)
        
    @property
    def LIVE_ACTORS(self):
        return self._linked_actors
    
    def isprocess(self):
        return False
    
    def manage_actors(self):
        """Remove actors not alive"""
        ACTORS = self.LIVE_ACTORS
        for aid,actor in list(iteritems(ACTORS)):
            if not actor.is_alive():
                ACTORS.pop(aid)
            else:
                self.on_manage_actor(actor)
                
    def spawn_actors(self):
        if self.num_workers:
            while len(self.LIVE_ACTORS) < self.num_workers:
                self.spawn_actor()
    

class Monitor(ActorPool):
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
adding a new monitor to the arbiter would follow the following pattern::

    import pulsar
    
    m = pulsar.arbiter().add_monitor(pulsar.Monitor,'mymonitor')

.. attribute:: worker_class

    The class derived form :class:`pulsar.Actor` which the monitor manages
    during its life time.
    
.. attribute:: num_workers

    The number of workers to monitor.
    
.. attribute:: LIVE_ACTORS

    dictionary containing instances of live actors

.. method:: manage_actors

    Remove actors which are not alive
    
.. method:: spawn_actors

    Spawn new actors as needed.
    
'''
    socket = None
    
    def _init(self, impl, worker_class, address = None, actor_params = None,
              actor_links = None, **kwargs):
        self.worker_class = worker_class
        self.address = address
        self._actor_params = actor_params
        self.actor_links = actor_links
        super(Monitor,self)._init(impl, **kwargs)
        
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
        return 'Monitor-{0}({1})'.format(self.worker_class.code(),self.aid)
    
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
        
    def stop_actor(self, actor):
        if not actor.is_alive():
            self.LIVE_ACTORS.pop(actor.aid)
        else:
            return actor.proxy.stop()
                
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
        
    def stop_actors(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if self.num_workers:
            num_to_kill = len(self.LIVE_ACTORS) - self.num_workers
            for i in range(num_to_kill, 0, -1):
                w, kage = 0, sys.maxsize
                for worker in iteritems(self.LIVE_ACTORS):
                    age = worker.age
                    if age < kage:
                        w, kage = w, age
                self.stop_actor(w)
            
    def spawn_actor(self):
        '''Spawn a new actor and add its :class:`pulsar.ActorProxyMonitor`
 to the :attr:`pulsar.Monitor.LIVE_ACTORS` dictionary.'''
        worker = self.arbiter.spawn(
                        self.worker_class,
                        monitor = self,
                        task_queue = self.task_queue,
                        actor_links = self.arbiter.get_all_monitors(),
                        **self.actorparams())
        monitor = self.arbiter.LIVE_ACTORS[worker.aid]
        self.LIVE_ACTORS[worker.aid] = monitor
        return worker
    
    def actorparams(self):
        '''Return a dictionary of parameters to be passed to the
spawn method when creating new actors.'''
        return self._actor_params or {}
        
    def info(self, full = False):
        if full:
            requests = []
            proxy = self.proxy
            for w in itervalues(self.LIVE_ACTORS):
                requests.append(proxy.info(w))
            return ActorCallBacks(self,requests).add_callback(self._info)
        else:
            return self._info()
        
    def _info(self, result = None):
        if not result:
            result = [a.local_info() for a in self.LIVE_ACTORS.values()] 
        tq = self.task_queue
        return {'worker_class':self.worker_class.code(),
                'workers': result,
                'num_workers':len(self.LIVE_ACTORS),
                'concurrency':self.cfg.concurrency,
                'listen':str(self.socket),
                'name':self.name,
                'age':self.age,
                'task_queue': tq is not None,
                'task_queue_size': tq.qsize() if tq else None}
        
    def get_actor(self, aid):
        '''Delegate get_actor to the arbiter'''
        return self.arbiter.get_actor(aid)
        
    