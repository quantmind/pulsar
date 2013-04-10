'''\
Pulsar ships with an asynchronous :class:`TaskQueue` built on top
:ref:`pulsar application framework <apps-framework>`. Task queues are used
as a mechanism to distribute work across threads/processes or machines.
Pulsar :class:`TaskQueue` is highly customizable, it can run in multi-threading
or multiprocessing (default) mode and can share :class:`task.Task` across
several machines.
By creating :class:`models.Job` classes in a similar way you do for celery_,
this application gives you all you need for running them with very
little setup effort::

    from pulsar.apps import tasks

    tq = tasks.TaskQueue(tasks_path=['path.to.tasks.*'])
    tq.start()
    
To get started, follow the these points:

* Create the script which runs your application, in the
  :ref:`taskqueue tutorial <tutorials-taskqueue>` the script is called
  ``manage.py``.
* Create the modules where :ref:`jobs <app-taskqueue-job>` are implemented. It
  can be a directory containing several submodules as explained in the
  :ref:`task paths parameter <app-tasks_path>`.
  

.. _app-taskqueue-job:

Configuration
~~~~~~~~~~~~~~~~
A :class:`TaskQueue` accepts several configuration parameters on top of the
standard :ref:`application settings <settings>`:

.. _app-tasks_path:

* The :ref:`task_paths <setting-task_paths>` parameter specify
  a list of python paths where to collect :class:`models.Job` classes::
  
      task_paths = ['myjobs','another.moduledir.*']
      
  The ``*`` at the end of the second module indicates to collect
  :class:`models.Job` from all submodules of ``another.moduledir``.
  
* The :ref:`schedule_periodic <setting-schedule_periodic>` flag indicates
  if the :class:`TaskQueue` can schedule :class:`models.PeriodicJob`. Usually,
  only one running :class:`TaskQueue` application is responsible for
  scheduling tasks while all the other, simply consume tasks.
  This parameter can also be specified in the command line via the
  ``--schedule-periodic`` flag. Default: ``False``.
  
* The :ref:`task_queue_factory <setting-task_queue_factory>` parameter
  is a dotted path to the callable which creates the
  :attr:`pulsar.apps.CPUboundApplication.queue` instance.
  By default, pulsar uses the :class:`pulsar.PythonMessageQueue` class
  which is a wrapper around the python :class:`multiprocessing.Queue` class.


.. _app-taskqueue-app:

Task queue application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TaskQueue
   :members:
   :member-order: bysource
   
   
.. _tasks-actions:

Task queue commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`TaskQueue` application adds the following
:ref:`internal commands <actor_commands>` which can be used to communicate
with the :class:`TaskQueue` monitor:

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


.. _celery: http://celeryproject.org/
'''
import os
from datetime import datetime

import pulsar
from pulsar import to_string
from pulsar.utils.log import local_property

from .exceptions import *
from .task import *
from .models import *
from .scheduler import Scheduler
from .states import *
from .rpc import *


class TaskSetting(pulsar.Setting):
    virtual = True
    app = 'tasks'


class TaskPaths(TaskSetting):
    name = "task_paths"
    section = "Task Consumer"
    validator = pulsar.validate_list
    default = ['pulsar.apps.tasks.testing']
    desc = """\
        List of python dotted paths where tasks are located.
        
        This parameter can only be specified during initialization or in a
        :ref:`config file <setting-config>`.
        """
        
        
class SchedulePeriodic(TaskSetting):
    name = 'schedule_periodic'
    section = "Task Consumer"
    flags = ["--schedule-periodic"]
    validator = pulsar.validate_bool
    action = "store_true"
    default = False
    desc = '''\
        Enable scheduling of periodic tasks.
        
        If enabled, :class:`pulsar.apps.tasks.PeriodicJob` will produce
        tasks according to their schedule.
        '''


class TaskQueue(pulsar.CPUboundApplication):
    '''A :class:`pulsar.apps.CPUboundApplication` for consuming
task.Tasks and managing scheduling of tasks via a
:class:`scheduler.Scheduler`.

.. attribute:: registry

    Instance of a :class:`models.JobRegistry` containing all
    registered :class:`models.Job` instances.
'''
    name = 'tasks'
    cfg = pulsar.Config(apps=('cpubound', 'tasks'),
                        timeout=600, backlog=5)
    task_class = TaskInMemory
    '''The :class:`Task` class for storing information about task execution.

Default: :class:`TaskInMemory`
'''
    '''The scheduler class. Default: :class:`Scheduler`.'''

    @local_property
    def scheduler(self):
        '''A :class:`scheduler.Scheduler` which sends tasks to the task queue
and produces periodic tasks according to their schedule of execution.

At every event loop, the :class:`pulsar.apps.ApplicationMonitor` running
the :class:`TaskQueue` application, invokes the :meth:`scheduler.Scheduler.tick`
method which check for tasks to be scheduled.

Check the :meth:`TaskQueue.monitor_task` callback
for implementation.'''
        if self.callable:
            self.callable() 
        return Scheduler(self.name,
                         self.queue,
                         self.task_class,
                         self.cfg.tasks_path,
                         logger=self.logger,
                         schedule_periodic=self.cfg.schedule_periodic)

    def request_instance(self, request):
        return self.scheduler.get_task(request)
    
    def monitor_task(self, monitor):
        '''Override the :meth:`pulsar.apps.Application.monitor_task` callback
to check if the :attr:`scheduler` needs to perform a new run.'''
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

    def actorparams(self, monitor, params):
        # Make sure we invoke super function so that we get the distributed
        # task queue
        params = super(TaskQueue, self).actorparams(monitor, params)
        # workers do not schedule periodic tasks
        params['app'].cfg.set('schedule_periodic', False)
        return params
     
    ############################################################################
    ##    INTERNALS        
    def _addtask(self, monitor, caller, jobname, task_extra, ack, args, kwargs):
        task = self.scheduler.queue_task(jobname, args, kwargs, **task_extra)
        if ack:
            return task
