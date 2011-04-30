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
    
    def __init__(self, task, args, kwargs, retries = 0, expires = None):
        self.time_executed = time()
        self.name = task.name
        self.id = task.make_task_id(args,kwargs)
        self.args = args or EMPTY_TUPLE
        self.kwargs = kwargs or EMPTY_DICT
        self.retries = retries
        self.expires = expires
        
    def on_start(self):
        timeout = self.revoked()
        self.timeout = timeout
        self.exception = timeout
        self.time_start = time()
        if timeout:
            self.time_end  = time()
            return False
        return True
    
    def on_finish(self, exception = None, result = None):
        self.exception = exception
        self.result = result
        if not self.time_end:
            self.time_end = time()
        
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
