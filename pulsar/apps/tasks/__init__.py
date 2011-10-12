'''\
A task-queue application for pulsar::

    from pulsar.apps import tasks
    
    tq = tasks.TaskQueue(tasks_path = 'path.to.tasks.*')
    tq.start()
    
An application implements several :class:`Job`
classes which specify the way each task is run.
Each job class is a task-factory, therefore, a task is always associated
with one job, which can be of two types:

* standard (:class:`Job`)
* periodic (:class:`PeriodicJob`)

To define a job is simple, subclass from :class:`Job` and implement the
callable function::

    from pulsar.apps import tasks
    
    class Addition(tasks.Job):
    
        def __call__(self, consumer, a, b):
            "Add two numbers"
            return a+b
            
The *consumer* is passed by the task queue, while the remaining arguments are
needed by your job implementation.

Task Class
================

By default, tasks are constructed using an in-memory implementation of
:class:`Task`. To use a different implementation, for example one that
saves tasks on a database, subclass :class:`Task` and pass the new class
to the :class:`TaskQueue` constructor::
    
    from pulsar.apps import tasks
    
    class TaskDatabase(tasks.Task):
        
        def on_created(self):
            return save2db(self)
        
        def on_received(self):
            return save2db(self)
            
        def on_start(self):
            return save2db(self)
            
        def on_finish(self):
            return save2db(self)
            
        @classmethod
        def get_task(cls, id, remove = False):
            return taskfromdb(id)
            
            
    tq = tasks.TaskQueue(task_class = TaskDatabase,
                         tasks_path = 'path.to.tasks.*')
    tq.start()


.. _tasks-callbacks:

Task callbacks
~~~~~~~~~~~~~~~~~~~

When creating your own task class all you need to override are the four
task callbacks:

* :meth:`Task.on_created` called by the taskqueue when it creates a new task
  instance.
* :meth:`Task.on_received` called by a worker when it receives the task.
* :meth:`Task.on_start` called by a worker when it starts the task.
* :meth:`Task.on_finish` called by a worker when it ends the task.


Task state
~~~~~~~~~~~~~

A :class:`Task` can have one of the following `status`:

* ``PENDING`` A task waiting for execution and unknown.
* ``RECEIVED`` when the task is received by the task queue.
* ``STARTED`` task execution has started.
* ``REVOKED`` the task execution has been revoked. One possible reason could be
  the task has timed out.
* ``SUCCESS`` task execution has finished with success.
* ``FAILURE`` task execution has finished with failure.
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
from .rpc import *


class TaskResponse(pulsar.Response):
    
    def __init__(self, worker, request):
        self.worker = worker
        super(TaskResponse,self).__init__(request)
        
    def close(self):
        task = self.request
        return task.finish(self.worker, result = task.result)


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
    
    def actor_save_task(self, caller, task):
        self.app.task_class.save_task(task)
    
    def actor_get_task(self, caller, id):
        return self.app.task_class.get_task(id)
    
    def actor_job_list(self, caller, jobnames = None):
        return list(self.app.job_list(jobnames = jobnames))
    
    def actor_next_scheduled(self, caller, jobname = None):
        return self.app.scheduler.next_scheduled(jobname = jobname)


class TaskQueue(pulsar.Application):
    '''A :class:`pulsar.Application` for consuming
tasks and managing scheduling of tasks.
    
.. attribute:: registry

    Instance of a :class:`JobRegistry` containing all
    registered :class:`Job` instances.
'''
    REMOVABLE_ATTRIBUTES = ('_scheduler',) +\
                             pulsar.Application.REMOVABLE_ATTRIBUTES
    task_class = TaskInMemory
    '''A subclass of :class:`Task` for storing information
    about task execution.
    
    Default: :class:`TaskInMemory`'''
    
    cfg = {'timeout':'3600'}
    
    _scheduler = None
    
    @property
    def scheduler(self):
        '''The scheduler is a producer of periodic tasks. At every event
loop of the :class:`pulsar.ApplicationMonitor` running the task queue
application, the application checks if a new periodic tasks need to
be scheduled. If so it makes the task requests.

Check the :meth:`TaskQueue.monitor_task` callback
for implementation.'''
        return self._scheduler
    
    def get_ioqueue(self):
        '''Return the distributed task queue which produces tasks to
be consumed by the workers.'''
        queue = self.cfg.task_queue_factory
        return queue()
    
    def __init__(self, task_class = None, **kwargs):
        self.tasks_statistics = {'total':0,
                                 'failures':0}
        self.task_class = task_class or self.task_class
        super(TaskQueue,self).__init__(**kwargs)
        
    def monitor_task(self, monitor):
        '''Override the :meth:`pulsar.Application.monitor_task` callback
to check if the schedulter needs to perform a new run.'''
        if self._scheduler:
            if self.scheduler.next_run <= datetime.now():
                self.scheduler.tick(monitor)
    
    def handler(self):
        # Load the application callable, the task consumer
        if self.callable:
            self.callable()
        import_modules(self.cfg.tasks_path)
        self._scheduler = Scheduler(self.task_class)
        return self
            
    def handle_request(self, worker, task):
        '''Called by the worker to perform the *task* in the queue.'''
        job = registry[task.name]
        with task.consumer(self,worker,job) as consumer:
            yield task.start(worker)
            task.result = job(consumer, *task.args, **task.kwargs)
        yield TaskResponse(worker,task)
            
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
