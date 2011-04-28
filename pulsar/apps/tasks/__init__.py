'''\
A task scheduler application with HTTP-RPC hooks
'''
import os
import pulsar
from time import time

import pulsar
from pulsar.utils.importer import import_modules

from .models import *
from .config import *
from .consumer import TaskConsumer
from .registry import registry


class TaskApplication(pulsar.Application):
    '''A task scheduler to be used with task Workers'''
    
    def get_task_queue(self):
        return pulsar.Queue()
    
    def init(self, parser = None, opts = None, args = None):
        self.cfg.worker_class = 'task'
        import_modules(self.cfg.tasks_path)
        
    def load(self):
        '''Load the application callable'''
        return TaskConsumer(self)
        
    def make_request(self, task_name, targs, tkwargs, **kwargs):
        '''Create a new Task Request'''
        if task_name in registry:
            task = registry[task_name]
            return TaskRequest(task, targs, tkwargs, **kwargs)
        else:
            raise TaskNotAvailable(task_name)
    
        
def createRpcTaskServer(rpchandle, **params):
    wsgi = pulsar.require('wsgi')
    tasks = pulsar.require('tasks')
    # Create the task server
    task_server = TaskApplication()
    rpc_server = wsgi.createServer(callable = rpchandle,
                                   links = {'task_server':task_server},
                                   **params)
    return rpc_server
