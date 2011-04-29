'''\
A task-queue application for pulsar::

    import pulsar
    
    tasks = pulsar.require('tasks')
    tq = tasks.TaskQueue()
    tq.start()
    
The usual input parameters apply.
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
    
    cfg = {'worker_class':'task',
           'timeout':'3600'}
    
    def get_task_queue(self):
        return pulsar.Queue()
    
    def init(self, parser = None, opts = None, args = None):
        self._scheduler = None
        import_modules(self.cfg.tasks_path)
        
    def load(self):
        # Load the application callable, the task consumer
        return TaskConsumer(self)
        
    def make_request(self, task_name, targs = None, tkwargs = None, **kwargs):
        '''Create a new Task Request'''
        return self.scheduler.make_request(task_name, targs, tkwargs, **kwargs)
        
    def monitor_task(self, monitor):
        if self.scheduler.next_run <= datetime.now():
            self.scheduler.tick(monitor.task_queue)

    @property
    def scheduler(self):
        '''The task queue scheduler is a task producer. At every event loop of the arbiter it checks
if new periodic tasks need to be scheduled. If so it makes the task requests.'''
        if not self._scheduler:
            self._scheduler = Scheduler()
        return self._scheduler
    
    @property
    def registry(self):
        global registry
        return registry
        

