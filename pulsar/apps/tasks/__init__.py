'''\
A task-queue application for pulsar::

    from pulsar.apps import tasks
    
    tq = tasks.TaskQueue(tasks_path = 'path.to.tasks.*')
    tq.start()
    
An application implements several :class:`pulsar.apps.tasks.Job`
classes which specify the way each task is run.
Each job class is task-factory, therefore, a task is always associated
with one job, which can be of two types:

* standard (:class:`Job`)
* periodic (:class:`PeriodicJob`)

To define a task is simple, subclass from :class:`Job` and implement the
callable function::

    from pulsar.apps import tasks
    
    class MyJob(tasks.Job):
    
        def __call__(self, consumer, *args, **kwargs):
            ...
            
The *consumer* is passed by the task queue.
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
from .scheduler import Scheduler
from .states import *
from .link import *
from .rpc import *


class TaskResponse(pulsar.Response):
    
    def __init__(self, worker, request):
        self.worker = worker
        super(TaskResponse,self).__init__(request)
        
    def close(self):
        task = self.request
        return task.on_finish(self.worker, result = task.result)


class Remotes(pulsar.ActorBase):
    
    def actor_tasks_list(self, caller):
        return self.app.tasks_list()
    
    def actor_addtask(self, caller, task_name, targs, tkwargs,
                      ack=True, **kwargs):
        return self.app._addtask(self, caller, task_name, targs, tkwargs,
                                    ack = True, **kwargs)
        
    def actor_addtask_noack(self, caller, task_name, targs, tkwargs,
                            ack=False, **kwargs):
        return self.app._addtask(self, caller, task_name, targs, tkwargs,
                                    ack = False, **kwargs)
    actor_addtask_noack.ack = False
    
    def actor_task_finished(self, caller, response):
        self.app.task_finished(response)
    actor_task_finished.ack = False
    
    def actor_get_task(self, caller, id):
        return self.app.get_task(id)
    
    def actor_job_list(self, caller, jobnames = None):
        return list(self.app.job_list(jobnames = jobnames))
    
    def actor_next_scheduled(self, caller, jobname = None):
        return self.app.scheduler.next_scheduled(jobname = jobname)


class TaskQueue(pulsar.Application):
    '''A :class:`pulsar.Application` for consuming
tasks and managing scheduling of tasks.
    
.. attribute:: registry

    Instance of a :class:`pulsar.apps.tasks.JobRegistry` containing all
    registered :class:`pulsar.apps.tasks.Job` instances.
'''
    REMOVABLE_ATTRIBUTES = ('scheduler',) +\
                             pulsar.Application.REMOVABLE_ATTRIBUTES
    task_class = TaskInMemory
    '''A subclass of :class:`pulsar.apps.tasks.Task` for storing information
    about task execution.
    
    Default: :class:`pulsar.apps.tasks.TaskInMemory`'''
    
    cfg = {'timeout':'3600'}
    
    @property
    def scheduler(self):
        '''The scheduler is a producer of periodic tasks. At every event
loop of the :class:`pulsar.ApplicationMonitor` running the task queue
application, the application checks if a new periodic tasks need to
be scheduled. If so it makes the task requests.

Check the :meth:`pulsar.apps.tasks.TaskQueue.monitor_task` callback
for implementation.'''
        if not hasattr(self,'_scheduler'):
            self._scheduler = Scheduler(self.task_class)
        return self._scheduler
    
    def get_task_queue(self):
        queue = self.cfg.task_queue_factory
        return queue()
    
    def __init__(self, task_class = None, **kwargs):
        self.task_class = task_class or self.task_class
        super(TaskQueue,self).__init__(**kwargs)
        
    def monitor_start(self, monitor):
        self.load()
        
    def monitor_task(self, monitor):
        '''Override the :meth:`pulsar.Application.monitor_task` callback
to check if the schedulter needs to perform a new run.'''
        if self.scheduler.next_run <= datetime.now():
            self.scheduler.tick(monitor)
        
    def load(self):
        # Load the application callable, the task consumer
        if self.callable:
            self.callable()
        import_modules(self.cfg.tasks_path)
        return self
            
    def handle_request(self, worker, task):
        '''Called by the worker to perform the *task* in the queue.'''
        job = registry[task.name]
        with task.consumer(self,worker,job) as consumer:
            task.on_start(worker)
            task.result = job(consumer, *task.args, **task.kwargs)
        return TaskResponse(worker,task)
            
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
    
    # Internals        
    def _addtask(self, monitor, caller, task_name, targs, tkwargs,
                 ack = True, **kwargs):
        task = self.scheduler.queue_task(monitor, task_name, targs, tkwargs,
                                         **kwargs)
        
        if ack:
            return task.tojson_dict()

    def remote_functions(self):
        return Remotes.remotes, Remotes.actor_functions
