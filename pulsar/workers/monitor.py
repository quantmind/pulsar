import os
import sys
import time
from multiprocessing import Pipe

import pulsar
from pulsar import getLogger, system
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

    def _init(self, app, num_workers = None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.address = self.cfg.address
        if self.address:
            self.socket = system.create_socket(self)
        else:
            self.socket = None
        super(WorkerMonitor,self)._init(self.cfg.worker_class,
                                        num_workers = self.cfg.workers or 1,
                                        **kwargs)
        
    @property
    def class_code(self):
        return '{0} - {1}'.format(self.worker_class.code(),self.app)
    
    @property
    def multithread(self):
        return self.worker_class.is_thread()
    
    @property
    def multiprocess(self):
        return self.worker_class.is_process()
    
    def clean_up(self):
        self.worker_class.clean_arbiter_loop(self,self.ioloop)
            
    def on_start(self):
        if self.socket:
            self.log.info("Listening at: {0}".format(self.socket))
        super(WorkerMonitor,self).on_start()
            
    def actor_params(self):
        '''Spawn a new worker'''
        return {'app':self.app,
                'socket': self.socket,
                'timeout': self.cfg.timeout,
                'impl': self.cfg.mode}
        
    def info(self):
        return {'worker_class':self.worker_class.code(),
                'workers':len(self.LIVE_ACTORS)}

    def configure_logging(self):
        self.app.configure_logging()
    