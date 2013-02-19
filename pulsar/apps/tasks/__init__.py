'''\
An asynchronous task-queue built on top :class:`pulsar.Application` framework.
By creating :class:`Job` classes in a similar way you can do for celery_,
this application gives you all you need for running them with very
little setup effort::

    from pulsar.apps import tasks

    tq = tasks.TaskQueue(tasks_path='path.to.tasks.*')
    tq.start()

.. _tasks-actions:

Tutorial
==============

Actions
~~~~~~~~~~~~~~~

The :class:`Taskqueue` application adds the following
:ref:`remote actions <api-remote_commands>` to its workers:

* **addtask** to add a new task to the task queue::

    send(taskqueue, 'addtask', jobname, task_extra, *args, **kwargs)

 * *jobname*: the name of the :class:`Job` to run.
 * *task_extra*: dictionary of extra parameters to pass to the :class:`Task`
   constructor. Usually a empty dictionary.
 * *args*: positional arguments for the :ref:`job callable <job-callable>`.
 * *kwargs*: key-valued arguments for the :ref:`job callable <job-callable>`.

* **addtask_noack** same as **addtask** but without acknowleding the sender::

    send(taskqueue, 'addtask_noack', jobname, task_extra, *args, **kwargs)
    
* **get_task** retrieve task information. This can be already executed or not.
  The implementation is left to the :meth:`Task.get_task` method::
  
    send(taskqueue, 'get_task', id)
    
* **get_tasks** retrieve information for tasks which satisfy the filtering.
  The implementation is left to the :meth:`Task.get_tasks` method::
  
    send(taskqueue, 'get_tasks', **filters)
  
    
Jobs
~~~~~~~~~~~~~~~~

An application implements several :class:`Job`
classes which specify the way each :class:`Task` is run.
Each job class is a task-factory, therefore, a task is always associated
with one job, which can be of two types:

* standard (:class:`Job`)
* periodic (:class:`PeriodicJob`)

.. _job-callable:

To define a job is simple, subclass from :class:`Job` and implement the
**job callable method**::

    from pulsar.apps import tasks

    class Addition(tasks.Job):

        def __call__(self, consumer, a, b):
            "Add two numbers"
            return a+b
            
    class Sampler(tasks.Job):

        def __call__(self, consumer, sample, size=10):
            ...

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


    tq = tasks.TaskQueue(task_class=TaskDatabase, tasks_path='path.to.tasks.*')
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


.. attribute:: FULL_RUN_STATES

    The set of states for which a :class:`Task` has run:
    ``FAILURE`` and ``SUCCESS``

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
from datetime import datetime

import pulsar
from pulsar import to_string

from .exceptions import *
from .task import *
from .models import *
from .scheduler import Scheduler
from .states import *
from .rpc import *


class TaskSetting(pulsar.Setting):
    virtual = True
    app = 'tasks'


class TaskPath(TaskSetting):
    name = "tasks_path"
    section = "Task Consumer"
    meta = "STRING"
    validator = pulsar.validate_list
    cli = ["--tasks-path"]
    default = ['pulsar.apps.tasks.testing']
    desc = """\
        List of python dotted paths where tasks are located.
        """
        

#################################################    TASKQUEUE COMMANDS
@pulsar.command()
def addtask(request, jobname, task_extra, *args, **kwargs):
    actor = request.actor
    kwargs.pop('ack', None)
    return actor.app._addtask(actor, request.caller, jobname, task_extra, True,
                              args, kwargs)

@pulsar.command()
def addtask_noack(request, jobname, task_extra, *args, **kwargs):
    actor = request.actor
    kwargs.pop('ack', None)
    return actor.app._addtask(actor, request.caller, jobname, task_extra, False,
                              args, kwargs)

@pulsar.command()
def save_task(request, task):
    return request.actor.app.scheduler.save_task(task)

@pulsar.command()
def delete_tasks(request, ids):
    return request.actor.app.scheduler.delete_tasks(ids)

@pulsar.command()
def get_task(request, id):
    return request.actor.app.scheduler.get_task(id)

@pulsar.command()
def get_tasks(request, **parameters):
    return request.actor.app.scheduler.get_tasks(**parameters)

@pulsar.command()
def job_list(request, jobnames=None):
    return list(request.actor.app.job_list(jobnames=jobnames))

@pulsar.command()
def next_scheduled(request, jobnames=None):
    return request.actor.app.scheduler.next_scheduled(jobnames=jobnames)

@pulsar.command()
def wait_for_task(request, id, timeout=3600):
    # wait for a task to finish for at most timeout seconds
    scheduler = request.actor.app.scheduler
    return scheduler.task_class.wait_for_task(scheduler, id, timeout)


class TaskQueue(pulsar.CPUboundApplication):
    '''A :class:`pulsar.CPUboundServer` for consuming
tasks and managing scheduling of tasks.

.. attribute:: registry

    Instance of a :class:`JobRegistry` containing all
    registered :class:`Job` instances.
'''
    _app_name = 'tasks'
    cfg_apps = ('cpubound',)
    cfg = {'timeout': '3600', 'backlog': 1}
    task_class = TaskInMemory
    '''The :class:`Task` class for storing information about task execution.

Default: :class:`TaskInMemory`
'''
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
        return self.local.scheduler

    def request_instance(self, request):
        return self.scheduler.get_task(request)

    def monitor_start(self, monitor):
        super(TaskQueue, self).monitor_start(monitor)
        self._create_scheduler()
        
    def worker_start(self, worker):
        super(TaskQueue, self).worker_start(worker)
        self._create_scheduler(False)
    
    def monitor_task(self, monitor):
        '''Override the :meth:`pulsar.Application.monitor_task` callback
to check if the scheduler needs to perform a new run.'''
        super(TaskQueue, self).monitor_task(monitor)
        s = self.scheduler
        if s:
            if s.next_run <= datetime.now():
                s.tick()

    def job_list(self, jobnames=None):
        return self.scheduler.job_list(jobnames=jobnames)

    @property
    def registry(self):
        global registry
        return registry

    ############################################################################
    ##    INTERNALS
    def _create_scheduler(self, schedule_periodic=True):
        # Load the application callable, the task consumer
        if self.callable:
            self.callable()
        self.local.scheduler = Scheduler(self.queue,
                                         self.task_class,
                                         self.cfg.tasks_path,
                                         logger=self.logger,
                                         schedule_periodic=schedule_periodic)
        
    def _addtask(self, monitor, caller, jobname, task_extra, ack, args, kwargs):
        task = self.scheduler.queue_task(jobname, args, kwargs, **task_extra)
        if ack:
            return task
