'''\
An asynchronous task-queue built on top :class:`pulsar.Application` framework.
By creating :class:`Job` classes in a similar way you can do for celery_,
this application gives you all you need for running them with very
little setup effort::

    from pulsar.apps import tasks
    
    tq = tasks.TaskQueue(tasks_path = 'path.to.tasks.*')
    tq.start()
    
Tutorial
================

Jobs
~~~~~~~~~~~~~~~~

An application implements several :class:`Job`
classes which specify the way each :class:`Task` is run.
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
            
The *consumer*, instance of :class:`TaskConsumer`, is passed by the
:class:`TaskQueue` and should always be the first positional argument in the
callable function.
The remaining positional arguments and/or key-valued parameters are needed by
your job implementation.

Task Class
~~~~~~~~~~~~~~~~~

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

When creating your own :class:`Task` class all you need to override are the four
task callbacks:

* :meth:`Task.on_created` called by the taskqueue when it creates a new task
  instance.
* :meth:`Task.on_received` called by a worker when it receives the task.
* :meth:`Task.on_start` called by a worker when it starts the task.
* :meth:`Task.on_finish` called by a worker when it ends the task.


and :meth:`Task.get_task` classmethod for retrieving tasks instances.

.. _task-state:

Task states
~~~~~~~~~~~~~

A :class:`Task` can have one of the following :attr:`Task.status` string:

* ``PENDING`` A task waiting for execution and unknown.
* ``RETRY`` A task is retrying calculation.
* ``RECEIVED`` when the task is received by the task queue.
* ``STARTED`` task execution has started.
* ``REVOKED`` the task execution has been revoked. One possible reason could be
  the task has timed out.
* ``UNKNOWN`` task execution is unknown.
* ``FAILURE`` task execution has finished with failure.
* ``SUCCESS`` task execution has finished with success.


.. attribute:: READY_STATES

    The set of states for which a :class:`Task` has finished:
    ``REVOKED``, ``FAILURE`` and ``SUCCESS``
    
    
Queue
~~~~~~~~~~~~~~

By default the queue is implemented using the multiprocessing.Queue
from the standard python library. To specify a different queue you can
use the ``task-queue`` flag from the command line::

    python myserverscript.py --task-queue dotted.path.to.callable
    
or by setting the ``task_queue_factory`` parameter in the config file
or in the :class:`TaskQueue` constructor.


.. _celery: http://celeryproject.org/
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
        '''Close the task request by invoking the :meth:`Task.finish`
method.'''
        task = self.request
        return task.finish(self.worker, result = task.result)


class Remotes(pulsar.ActorBase):
    
    def actor_tasks_list(self, caller):
        return self.app.tasks_list()
    
    def actor_addtask(self, caller, jobname, task_extra, *args, **kwargs):
        kwargs.pop('ack',None)
        return self.app._addtask(self, caller, jobname, task_extra, True,
                                 args, kwargs)
        
    def actor_addtask_noack(self, caller, jobname, task_extra, *args, **kwargs):
        kwargs.pop('ack',None)
        return self.app._addtask(self, caller, jobname, task_extra, False,
                                 args, kwargs)
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
    app = 'tasks'
    cfg = {'timeout':'3600'}
    task_class = TaskInMemory
    '''The :class:`Task` class for storing information about task execution.
    
Default: :class:`TaskInMemory`
'''
    scheduler_class = Scheduler
    '''The scheduler class. Default: :class:`Scheduler`.'''
    
    @property
    def scheduler(self):
        '''A :class:`Scheduler` which send task to the task queue and
produces of periodic tasks according to their schedule of execution.

At every event loop, the :class:`pulsar.ApplicationMonitor` running
the :class:`TaskQueue` application, invokes the :meth:`Scheduler.tick`
which check for tasks to be scheduled.

Check the :meth:`TaskQueue.monitor_task` callback
for implementation.'''
        return self.local.get('scheduler')
        
    def get_ioqueue(self):
        '''Return the distributed task queue which produces tasks to
be consumed by the workers.'''
        queue = self.cfg.task_queue_factory
        return queue()
    
    def request_instance(self, request):
        try:
            return self.task_class.from_queue(request)
        except:
            self.log.critical('Could not retrieve task "{0}"'.format(request))
            raise
    
    def __init__(self, task_class = None, **kwargs):
        self.tasks_statistics = {'total':0,
                                 'failures':0}
        self.task_class = task_class or self.task_class
        super(TaskQueue,self).__init__(**kwargs)
        
    def monitor_task(self, monitor):
        '''Override the :meth:`pulsar.Application.monitor_task` callback
to check if the scheduler needs to perform a new run.'''
        s = self.scheduler
        if s:
            if s.next_run <= datetime.now():
                s.tick(monitor)
    
    def handler(self):
        # Load the application callable, the task consumer
        if self.callable:
            self.callable()
        import_modules(self.cfg.tasks_path)
        self.local['scheduler'] = Scheduler(self.task_class)
        return self
    
    def monitor_handler(self):
        return self.handler()
            
    def handle_request(self, worker, task):
        '''Called by the worker to perform the *task* in the queue.'''
        job = registry[task.name]
        with task.consumer(self,worker,job) as consumer:
            yield task.start(worker)
            task.result = job(consumer, *task.args, **task.kwargs)
        yield TaskResponse(worker, task)
            
    def job_list(self, jobnames = None):
        return self.scheduler.job_list(jobnames = jobnames)
    
    @property
    def registry(self):
        global registry
        return registry
    
    # Internals
    def _addtask(self, monitor, caller, jobname, task_extra, ack, args, kwargs):
        task = self.scheduler.queue_task(monitor, jobname, args, kwargs,
                                         **task_extra)
        if ack:
            return task

    def remote_functions(self):
        return Remotes.remotes, Remotes.actor_functions
    
    def on_actor_message_processed(self, message, result):
        '''handle the messaged processed callback when the message action
is either "addtask" or "addtask_noack".
When that is the case, the application broadcast the task id associated with
the message request id.'''
        if message.action in ('addtask','addtask_noack'):
            pass

    def broadcast(self, what, task):
        ''''''
        pass