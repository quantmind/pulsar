'''\
A task-queue application for pulsar::

    import pulsar
    
    tasks = pulsar.require('tasks')
    tq = tasks.TaskQueue(tasks_path = 'path.to.tasks.*')
    tq.start()
    
The usual input parameters apply.
'''
import os
import pulsar
from time import time
from datetime import datetime

import pulsar
from pulsar.utils.importer import import_modules

from .exceptions import *
from .config import *
from .task import *
from .models import *
from .scheduler import *
from .worker import TaskScheduler
from .states import *


class TaskQueue(pulsar.Application):
    '''A task queue application for consuming task and scheduling.'''
    monitor_class = TaskScheduler
    REMOVABLE_ATTRIBUTES = ('scheduler',) + pulsar.Application.REMOVABLE_ATTRIBUTES
    task_class = TaskInMemory
    
    cfg = {'worker_class':'pulsar.apps.tasks.worker.Worker',
           'timeout':'3600'}
    
    def get_task_queue(self):
        return pulsar.Queue()
    
    def __init__(self, task_class = None, **kwargs):
        self.task_class = task_class or self.task_class
        super(TaskQueue,self).__init__(**kwargs)
        
    def init(self, parser = None, opts = None, args = None):
        self._scheduler = None
        self.load()
        
    def load(self):
        # Load the application callable, the task consumer
        if self.callable:
            self.callable()
        import_modules(self.cfg.tasks_path)
        return self
        
    def make_request(self, task_name, targs = None, tkwargs = None, **kwargs):
        '''Create a new Task Request'''
        return self.scheduler.make_request(task_name, targs, tkwargs, **kwargs)
        
    def monitor_task(self, monitor):
        if self.scheduler.next_run <= datetime.now():
            self.scheduler.tick(monitor.task_queue)
            
    def handle_event_task(self, worker, task):
        if task.on_start(worker):
            job = registry[task.name]
            try:
                result = job(self, *task.args, **task.kwargs)
            except Exception as e:
                result = TaskException(e,log = worker.log)
            return task, result
        else:
            return task, TaskTimeout(task.name,task.expires)

    def end_event_task(self, worker, task, result):
        if isinstance(result,Exception):
            task.on_finish(worker, exception = result)
        else:
            task.on_finish(worker, result = result)
            
    def task_finished(self, response):
        response._on_finish()
        
    def get_task(self, id):
        return self.task_class.get_task(id)
    
    def job_list(self):
        return self.scheduler.job_list()

    @property
    def scheduler(self):
        '''The task queue scheduler is a task producer. At every event loop of the arbiter it checks
if new periodic tasks need to be scheduled. If so it makes the task requests.'''
        if not self._scheduler:
            self._scheduler = Scheduler(self.task_class)
        return self._scheduler
    
    @property
    def registry(self):
        global registry
        return registry
        
