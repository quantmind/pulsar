'''\
A task-queue application for pulsar::

    import pulsar
    
    tasks = pulsar.require('tasks')
    tq = tasks.TaskQueue(tasks_path = 'path.to.tasks.*')
    tq.start()
    
An application implements several :class:`pulsar.apps.tasks.Job`
classes which specify the way each task is run.
A job class is used to generate a series of tasks.

Therefore, a task is always associated with a job, which can be
of two types:

* standard
* periodic (uses a scheduler)
'''
import os
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
from .link import *
from .rpc import *


class TaskQueue(pulsar.Application):
    '''A :class:`pulsar.Application` for consuming
tasks and managing scheduling of tasks.
   
.. attribute: task_class

    A subclass of :class:`pulsar.apps.tasks,Task` for storing information
    about task execution.
'''
    monitor_class = TaskScheduler
    REMOVABLE_ATTRIBUTES = ('scheduler',) +\
                             pulsar.Application.REMOVABLE_ATTRIBUTES
    task_class = TaskInMemory
    
    cfg = {'worker_class':'pulsar.apps.tasks.worker.Worker',
           'timeout':'3600'}
    
    @property
    def scheduler(self):
        '''The scheduler is a producer of periodic tasks. At every event
loop of the :class:`pulsar.Monitor` running the task queue application
checks if a new periodic tasks need to be scheduled.
If so it makes the task requests.'''
        if not self._scheduler:
            self._scheduler = Scheduler(self.task_class)
        return self._scheduler
    
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
        
    def make_request(self, job_name, targs = None, tkwargs = None, **kwargs):
        '''Create a new task request. This function delegate the
responsability to the :attr:`pulsar.apps.tasks.TaskQueue.scheduler`

:parameter job_name: the name of a :class:`pulsar.apps.tasks.Job` registered
    with the application.
:parameter targs: optional tuple of arguments for the task.
:parameter tkwargs: optional dictionary of arguments for the task.'''
        return self.scheduler.make_request(job_name, targs, tkwargs, **kwargs)
        
    def monitor_task(self, monitor):
        if self.scheduler.next_run <= datetime.now():
            self.scheduler.tick(monitor.task_queue)
            
    def handle_event_task(self, worker, task):
        '''Called by the worker to perform the *task* in the queue.'''
        job = registry[task.name]
        with task.consumer(self,worker,job) as consumer:
            task.on_start(worker)
            task.result = job(consumer, *task.args, **task.kwargs)
        return task, task.result

    def end_event_task(self, worker, task, result):
        task.on_finish(worker, result = result)
            
    def task_finished(self, response):
        response._on_finish()
        
    def get_task(self, id):
        return self.task_class.get_task(id)
    
    def job_list(self, jobnames = None):
        return self.scheduler.job_list(jobnames = jobnames)
    
    @property
    def registry(self):
        global registry
        return registry
        
