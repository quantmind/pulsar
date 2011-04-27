import os
import sys
import time

import pulsar
from pulsar.utils.py2py3 import iteritems

from .actor import Actor
from .defer import make_deferred


__all__ = ['Monitor','ActorPool']


class ActorPool(Actor):
    
    def _init(self, impl, *args, **kwargs):
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
        for aid,actor in list(iteritems(self.LIVE_ACTORS)):
            if not actor.is_alive():
                ACTORS.pop(aid)
            else:
                self.on_manage_actor(actor)
                
    def spawn_actors(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        if self.num_workers:
            while len(self.LIVE_ACTORS) < self.num_workers:
                self.spawn_actor()        
    

class Monitor(ActorPool):
    '''\
A monitor is a special :class:`pulsar.Actor` which shares
the same event loop as the :class:`pulsar.Arbiter`
and therefore lives in the main process.
A monitor manages a set of actors.

.. attribute: worker_class

    a Worker class derived form :class:`pulsar.Arbiter`
    
.. attribute: num_workers

    The number of workers to monitor.

'''
    def _init(self, impl, worker_class, address = None, **kwargs):
        self.worker_class = worker_class
        self.worker_age = 0
        self.address = address
        self.task_queue = self.worker_class.get_task_queue(self)
        super(Monitor,self)._init(impl, **kwargs)
    
    # HOOKS
    def on_start(self):
        self.worker_class.modify_arbiter_loop(self)
        if not hasattr(self,'socket'):
            self.socket = None
        if self.socket:
            self.log.info("Listening at: {0}".format(self.socket))
        
    def on_task(self):
        self.manage_actors()
        if not self._stopping:
            self.spawn_actors()
            self.stop_actors()
        
    def on_stop(self):
        '''Close the Pool.'''
        for actor in self.linked_actors():
            actor.stop()
        
    # OVERRIDES
    
    @property
    def name(self):
        return '{1}-{0}({2})'.format(self.worker_class.code(),self.app,self.aid[:8])
    
    def _get_eventloop(self, impl):
        return self.arbiter.ioloop
    
    def _stop_ioloop(self):
        return make_deferred()
    
    def _run(self):
        pass
    
    @property
    def multithread(self):
        return self.cfg.concurrency == 'thread'
    @property
    def multiprocess(self):
        return self.cfg.concurrency == 'process'
    
    def clean_up(self):
        self.worker_class.clean_arbiter_loop(self,self.ioloop)
        
    def stop_actor(self, actor):
        if not actor.is_alive():
            self.LIVE_ACTORS.pop(actor.aid)
        else:
            return actor.proxy.stop()
                
    def __join(self, timeout = 1):
        '''Join the pool, close or terminate must have been called before.'''
        if not self.stopped():
            raise ValueError('Cannot join worker pool. Must be stopped or terminated first.')
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
        '''Spawn a new worker'''
        self.worker_age += 1
        worker = self.arbiter.spawn(self.worker_class,
                                    monitor = self,
                                    age = self.worker_age,
                                    task_queue = self.task_queue,
                                    **self.actor_params())
        monitor = self.arbiter.LIVE_ACTORS[worker.aid]
        self.LIVE_ACTORS[worker.aid] = monitor
        return worker
    
    def actor_params(self):
        return {}
        
    def info(self):
        return {'worker_class':self.worker_class.code(),
                'workers':len(self.LIVE_ACTORS)}

    