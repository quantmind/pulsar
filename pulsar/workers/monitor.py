import os
import sys
import time
from multiprocessing import Pipe

import pulsar
from pulsar.utils.py2py3 import iteritems


__all__ = ['WorkerMonitor']

    
class WorkerMonitor(pulsar.Monitor):
    '''\
A pool of worker classes for performing asynchronous tasks and input/output

:parameter arbiter: the arbiter managing the pool. 
:parameter worker_class: a Worker class derived form :class:`pulsar.Worker`
:parameter num_workers: The number of workers in the pool.
:parameter app: The working application
:parameter timeout: Timeout in seconds for murdering unresponsive workers 
    '''

    def _init(self, impl, app, num_workers = None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        if self.cfg.concurrency == 'process':
            self.task_queue = None
        else:
            self.task_queue = pulsar.ThreadQueue()
        super(WorkerMonitor,self)._init(impl,
                                        self.cfg.worker_class,
                                        address = self.cfg.address,
                                        num_workers = self.cfg.workers or 1,
                                        **kwargs)
    
    @property
    def multithread(self):
        return self.worker_class.is_thread()
    
    @property
    def multiprocess(self):
        return self.worker_class.is_process()
    
    def clean_up(self):
        self.worker_class.clean_arbiter_loop(self,self.ioloop)
            
    def actor_params(self):
        '''Spawn a new worker'''
        return {'app':self.app,
                'socket': self.socket,
                'timeout': self.cfg.timeout,
                'loglevel': self.app.loglevel,
                'impl': self.cfg.concurrency,
                'task_queue': self.task_queue}
        
    def info(self):
        return {'worker_class':self.worker_class.code(),
                'workers':len(self.LIVE_ACTORS)}

    def configure_logging(self, **kwargs):
        self.app.configure_logging(**kwargs)
        self.loglevel = self.app.loglevel
    