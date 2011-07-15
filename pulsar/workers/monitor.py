import os
import sys
import time
from multiprocessing import Pipe

import pulsar
from pulsar.utils.py2py3 import iteritems


__all__ = ['WorkerMonitor']

    
class WorkerMonitor(pulsar.Monitor):
    '''A :class:`pulsar.Monitor` implementation
for :class:`pulsar.Application`.'''
    def _init(self, impl, app, num_workers = None, **kwargs):
        self.app = app
        self.cfg = app.cfg
        super(WorkerMonitor,self)._init(impl,
                                        self.cfg.worker_class,
                                        address = self.cfg.address,
                                        num_workers = self.cfg.workers or 1,
                                        **kwargs)
    
    def on_task(self):
        super(WorkerMonitor,self).on_task()
        if not self._stopping:
            self.app.monitor_task(self)
            
    def on_exit(self):
        self.app.on_exit(self)
        
    def clean_up(self):
        self.worker_class.clean_arbiter_loop(self,self.ioloop)
            
    def actor_params(self):
        '''Parameters to be passed to the spawn method
when creating new actors.'''
        return {'app':self.app,
                'socket': self.socket,
                'timeout': self.cfg.timeout,
                'loglevel': self.app.loglevel,
                'impl': self.cfg.concurrency}

    def configure_logging(self, **kwargs):
        self.app.configure_logging(**kwargs)
        self.loglevel = self.app.loglevel
        
    def _info(self, result = None):
        info = super(WorkerMonitor,self)._info(result)
        info.update({'default_timeout': self.cfg.timeout})
        return info

