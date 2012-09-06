'''\
An asynchronous task-queue built on top :class:`pulsar.Application` framework.
By creating :class:`Job` classes in a similar way you can do for celery_,
this application gives you all you need for running them with very
little setup effort::

    from pulsar.apps import tasks

    tq = tasks.TaskQueue(tasks_path='path.to.tasks.*')
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
from pulsar import to_string, safe_async
from pulsar.async.commands import authenticated, pulsar_command, internal
from pulsar.utils.importer import import_modules, module_attribute

from .exceptions import *
from .task import *
from .models import *
from .scheduler import Scheduler
from .states import *
from .rpc import *

def validate_list(val):
    if isinstance(val,list):
        return val
    elif isinstance(val,tuple):
        return list(val)
    else:
        val = to_string(val).split(',')
        vals = []
        for v in to_string(val).split(','):
            v = v.strip()
            if v:
                vals.append(v)
        return vals


class TaskQueueFactory(pulsar.Setting):
    app = 'cpubound'
    name = "task_queue_factory"
    section = "Task Consumer"
    flags = ["-q", "--task-queue"]
    default = "pulsar.Queue"
    desc = """The task queue factory to use."""

    def get(self):
        return module_attribute(self.value)


class TaskSetting(pulsar.Setting):
    virtual = True
    app = 'tasks'


class TaskPath(TaskSetting):
    name = "tasks_path"
    section = "Task Consumer"
    meta = "STRING"
    validator = validate_list
    cli = ["--tasks-path"]
    default = ['pulsar.apps.tasks.testing']
    desc = """\
        List of python dotted paths where tasks are located.
        """


class CPUboundServer(pulsar.Application):
    '''A CPU-bound application server.'''
    _app_name = 'cpubound'

    def get_ioqueue(self):
        '''Return the distributed task queue which produces tasks to
be consumed by the workers.'''
        return self.cfg.task_queue_factory()

    def request_instance(self, worker, fd, request):
        return request

    def on_event(self, worker, fd, request):
        request = self.request_instance(worker, fd, request)
        if request is not None:
            c = self.local.current_requests
            if c is None:
                c = []
                self.local.current_requests = c
            c.append(request)
            yield safe_async(request.start, args=(worker,))
            try:
                c.remove(request)
            except ValueError:
                pass

#################################################    TASKQUEUE COMMANDS
taskqueue_cmnds = set()

@pulsar_command(internal=True, authenticated=True, commands_set=taskqueue_cmnds)
def addtask(client, actor, caller, jobname, task_extra, *args, **kwargs):
    '''Add a new task to the task queue.

:parameter jobname: the job to run
:parameter task_extra: Dictionary of extra parameters to pass to the Task
    class constructor. Usually a empty dictionary.
:parameter args: positional arguments for the callable Job.
:parameter kwargs: keyed-valued arguments for the callable Job.
'''
    kwargs.pop('ack', None)
    return actor.app._addtask(actor, caller, jobname, task_extra, True,
                              args, kwargs)

@pulsar_command(internal=True, ack=False, commands_set=taskqueue_cmnds)
def addtask_noack(client, actor, caller, jobname, task_extra, *args, **kwargs):
    kwargs.pop('ack', None)
    return actor.app._addtask(actor, caller, jobname, task_extra, False,
                              args, kwargs)

@pulsar_command(internal=True, commands_set=taskqueue_cmnds)
def save_task(client, actor, caller, task):
    #import time
    #time.sleep(0.1)
    return actor.app.scheduler.save_task(task)

@pulsar_command(internal=True, commands_set=taskqueue_cmnds)
def delete_tasks(client, actor, caller, ids):
    return actor.app.scheduler.delete_tasks(ids)

@pulsar_command(commands_set=taskqueue_cmnds)
def get_task(client, actor, id):
    return actor.app.scheduler.get_task(id)

@pulsar_command(commands_set=taskqueue_cmnds)
def job_list(client, actor, jobnames=None):
    return list(actor.app.job_list(jobnames=jobnames))

@pulsar_command(commands_set=taskqueue_cmnds)
def next_scheduled(client, actor, jobnames=None):
    return actor.app.scheduler.next_scheduled(jobnames=jobnames)


class TaskQueue(CPUboundServer):
    '''A :class:`pulsar.CPUboundServer` for consuming
tasks and managing scheduling of tasks.

.. attribute:: registry

    Instance of a :class:`JobRegistry` containing all
    registered :class:`Job` instances.
'''
    _app_name = 'tasks'
    cfg_apps = ('cpubound',)
    cfg = {'timeout': '3600', 'backlog': 1}
    commands_set = taskqueue_cmnds
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
        return self.local.scheduler

    def request_instance(self, worker, fd, request):
        return self.scheduler.get_task(request)

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
        self.local.scheduler = Scheduler(self.task_class)
        return self

    def monitor_handler(self):
        return self.handler()

    def job_list(self, jobnames=None):
        return self.scheduler.job_list(jobnames=jobnames)

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
