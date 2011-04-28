'''\
A task scheduler application with HTTP-RPC hooks
'''
import os
import pulsar
from time import time
from datetime import datetime

import pulsar
from pulsar.utils.importer import import_modules

from .models import *
from .config import *
from .consumer import TaskConsumer
from .scheduler import Scheduler
from .registry import registry


class TaskQueue(pulsar.Application):
    '''A task queue application for consuming task and scheduling.'''
    REMOVABLE_ATTRIBUTES = ('scheduler',) + pulsar.Application.REMOVABLE_ATTRIBUTES
    
    cfg = {'worker_class':'task'}
    
    def get_task_queue(self):
        return pulsar.Queue()
    
    def init(self, parser = None, opts = None, args = None):
        self._scheduler = None
        import_modules(self.cfg.tasks_path)
        
    def load(self):
        # Load the application callable, the task consumer
        return TaskConsumer(self)
        
    def make_request(self, task_name, targs, tkwargs, **kwargs):
        '''Create a new Task Request'''
        return self.scheduler.make_request(task_name, targs, tkwargs, **kwargs)
        
    def monitor_task(self, monitor):
        if self.scheduler.next_run <= datetime.now():
            self.scheduler.tick(monitor.task_queue)

    @property
    def scheduler(self):
        if not self._scheduler:
            self._scheduler = Scheduler()
        return self._scheduler
    
    @property
    def registry(self):
        global registry
        return registry
        
        
def createRpcTaskServer(rpchandle, **params):
    wsgi = pulsar.require('wsgi')
    tasks = pulsar.require('tasks')
    # Create the task server
    task_server = TaskQueue()
    rpc_server = wsgi.createServer(callable = rpchandle,
                                   links = {'task_server':task_server},
                                   **params)
    return rpc_server
