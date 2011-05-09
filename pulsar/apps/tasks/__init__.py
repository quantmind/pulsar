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

from .models import *
from .config import *
from .scheduler import Scheduler
from .consumer import TaskRequestMemory
from .registry import registry
from .worker import TaskScheduler
from .exceptions import *


def get_request_class(name = None):
    if name == 'stdnet':
        from .db.stdn.taskqueue.models import StdnetTaskRequest
        return StdnetTaskRequest
    else:
        return TaskRequestMemory


class TaskQueue(pulsar.Application):
    '''A task queue application for consuming task and scheduling.'''
    monitor_class = TaskScheduler
    REMOVABLE_ATTRIBUTES = ('scheduler',) + pulsar.Application.REMOVABLE_ATTRIBUTES
    request_class = None
    
    cfg = {'worker_class':'pulsar.apps.tasks.worker.Worker',
           'timeout':'3600'}
    
    def get_task_queue(self):
        return pulsar.Queue()
    
    def __init__(self, request_class = None, **kwargs):
        self.request_class = request_class or self.request_class
        super(TaskQueue,self).__init__(**kwargs)
        
    def init(self, parser = None, opts = None, args = None):
        self._scheduler = None
        self.load()
        
    def load(self):
        # Load the application callable, the task consumer
        import_modules(self.cfg.tasks_path)
        return self
        
    def make_request(self, task_name, targs = None, tkwargs = None, **kwargs):
        '''Create a new Task Request'''
        return self.scheduler.make_request(task_name, targs, tkwargs, **kwargs)
        
    def monitor_task(self, monitor):
        if self.scheduler.next_run <= datetime.now():
            self.scheduler.tick(monitor.task_queue)
            
    def handle_event_task(self, worker, request):
        if request.on_start(worker):
            task = registry[request.name]
            try:
                result = task(self, *request.args, **request.kwargs)
            except Exception as e:
                result = TaskException(str(e))
            return request, result
        else:
            return request, TaskTimeout(request.name,request.expires)

    def end_event_task(self, worker, response, result):
        if isinstance(result,Exception):
            response.on_finish(worker, exception = result)
        else:
            response.on_finish(worker, result = result)
            
    def task_finished(self, response):
        response._on_finish()
        
    def get_task(self, id):
        return self.scheduler.TaskRequest.get_task(id)

    @property
    def scheduler(self):
        '''The task queue scheduler is a task producer. At every event loop of the arbiter it checks
if new periodic tasks need to be scheduled. If so it makes the task requests.'''
        if not self._scheduler:
            self._scheduler = Scheduler(get_request_class(self.request_class))
        return self._scheduler
    
    @property
    def registry(self):
        global registry
        return registry
        
