'''\
A task scheduler application with HTTP-RPC hooks
'''
import os
import pulsar
from time import time
from pulsar.utils.importer import import_modules

from .models import *
from .config import *
from .exceptions import *
from .registry import registry

EMPTY_TUPLE = ()
EMPTY_DICT = {}
        

class TaskRequest(object):
    time_start    = None
    time_end      = None
    result        = None
    exception     = None
    timeout       = False
    _already_revoked = False
    
    def __init__(self, task, args, kwargs, retries = 0,
                 expires = None, ack = True):
        self.time_executed = time()
        self.name = task.name
        self.ack = ack
        self.id = task.make_task_id(args,kwargs)
        tc = self.get_task(self.id)
        if tc:
            self._already_revoked = True
            #if tc.done():
            #    self._already_revoked = self.revoke_on_same_id()
            #else:
            #    self._already_revoked = True
        self.args = args or EMPTY_TUPLE
        self.kwargs = kwargs or EMPTY_DICT
        self.retries = retries
        self.expires = expires
        self.time_executed = time()
        if not self.revoked():
            self._on_init()
    
    def revoke_on_same_id(self):
        return True
        
    def _on_init(self):
        pass
    
    def _on_start(self,worker):
        pass
    
    def _on_finish(self,worker):
        pass
    
    def on_start(self,worker):
        timeout = self.revoked()
        self.timeout = timeout
        self.exception = timeout
        self.time_start = time()
        if timeout:
            self.time_end  = time()
            return False
        self._on_start(worker)
        return True
    
    def on_finish(self, worker, exception = None, result = None):
        self.exception = exception
        self.result = result
        if not self.time_end:
            self.time_end = time()
        self._on_finish(worker)
        
    def done(self):
        if self.time_end:
            return True
        
    def maybe_expire(self):
        if self.expires and time() > self.expires:
            return True
    
    def revoked(self):
        if self._already_revoked:
            return True
        if self.expires:
            return self.maybe_expire()
        return False
    
    def execute2start(self):
        if self.time_start:
            return self.time_start - self.time_executed
        
    def execute2end(self):
        if self.time_end:
            return self.time_end - self.time_executed
        
    def duration(self):
        if self.time_end:
            return self.time_end - self.time_start  

    def todict(self):
        return self.__dict__.copy()
    
    @classmethod
    def get_task(cls, id, remove = False):
        raise NotImplementedError
    
    def ack(self):
        return self
    

class TaskRequestMemory(TaskRequest):
    _TASKS = {}
    _TASKS_DONE = {}
    
    def _on_init(self):
        self._TASKS[self.id] = self
        
    def _on_start(self,worker):
        self._TASKS[self.id] = self
    
    def _on_finish(self,worker=None):
        if worker:
            worker.monitor.send(worker.aid, ((self,),EMPTY_DICT), name = 'task_finished')
        else:
            self._TASKS[self.id] = self

    @classmethod
    def get_task(cls,id,remove=False):
        task = cls._TASKS.get(id,None)
        if remove and task:
            if task.done():
                cls._TASKS.pop(id)
        return task
