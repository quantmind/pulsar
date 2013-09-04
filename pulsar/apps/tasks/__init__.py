'''\
Pulsar ships with an asynchronous :class:`TaskQueue` built on top
:ref:`pulsar application framework <apps-framework>`. Task queues are used
as a mechanism to distribute work across threads/processes or machines.
Pulsar :class:`TaskQueue` is highly customizable, it can run in multi-threading
or multiprocessing (default) mode and can share :class:`backends.Task` across
several machines.
By creating :class:`models.Job` classes in a similar way you do for celery_,
this application gives you all you need for running them with very
little setup effort::

    from pulsar.apps import tasks

    if __name__ == '__main__':
        tasks.TaskQueue(tasks_path=['path.to.tasks.*']).start()
        
Check the :ref:`task queue tutorial <tutorials-taskqueue>` for a running
example with simple tasks.
    
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
  scheduling tasks while any the other, simply consume tasks.
  
  It can be specified in the command line via the
  ``--schedule-periodic`` flag.
  
  Default: ``False``.
  
* The :ref:`task_backend <setting-task_backend>` parameter is a url
  type string which specifies the :ref:`task backend <apps-taskqueue-backend>`
  to use.
  
  It can be specified in the command line via the
  ``--task-backend ...`` option.
  
  Default: ``local://``.

* The :ref:`concurrent_tasks <setting-concurrent_tasks>` parameter control
  the maximum number of concurrent tasks for a given task worker.
  This parameter is important when tasks are asynchronous, that is when
  they perform some sort of I/O and the :ref:`job callable <job-callable>`
  returns and :ref:`asynchronous component <tutorials-coroutine>`.
  
  It can be specified in the command line via the
  ``--concurrent-tasks ...`` option.
  
  Default: ``5``.
  
.. _app-taskqueue-app:

Task queue application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TaskQueue
   :members:
   :member-order: bysource
   

.. _celery: http://celeryproject.org/
'''
from datetime import datetime

import pulsar
from pulsar import command
from pulsar.utils.config import section_docs

from .models import *
from .states import *
from .backends import *
from .rpc import *


section_docs['Task Consumer'] = '''
This section covers configuration parameters used by CPU bound type applications
such as the :ref:`distributed task queue <apps-taskqueue>` and the
:ref:`test suite <apps-test>`.'''


class TaskSetting(pulsar.Setting):
    virtual = True
    app = 'tasks'
    section = "Task Consumer"


class ConcurrentTasks(TaskSetting):
    name = "concurrent_tasks"
    flags = ["--concurrent-tasks"]
    validator = pulsar.validate_pos_int
    type = int
    default = 5
    desc = """\
        The maximum number of concurrent tasks for a worker.
        
        When a task worker reach this number it stops polling for more tasks
        until one or more task finish. It should only affect task queues under
        significant load.
        Must be a positive integer. Generally set in the range of 5-10.
        """

class TaskBackendConnection(TaskSetting):
    name = "task_backend"
    flags = ["--task-backend"]
    default = "local://"
    desc = '''\
        Task backend.

        A task backend is string which connect to the backend storing Tasks)
        which accepts one parameter only and returns an instance of a
        distributed queue which has the same API as
        :class:`pulsar.MessageQueue`. The only parameter passed to the
        task queue factory is a :class:`pulsar.utils.config.Config` instance.
        This parameters is used by :class:`pulsar.apps.tasks.TaskQueue`
        application.'''

    
class TaskPaths(TaskSetting):
    name = "task_paths"
    validator = pulsar.validate_list
    default = []
    desc = """\
        List of python dotted paths where tasks are located.
        
        This parameter can only be specified during initialization or in a
        :ref:`config file <setting-config>`.
        """
        
        
class SchedulePeriodic(TaskSetting):
    name = 'schedule_periodic'
    flags = ["--schedule-periodic"]
    validator = pulsar.validate_bool
    action = "store_true"
    default = False
    desc = '''\
        Enable scheduling of periodic tasks.
        
        If enabled, :class:`pulsar.apps.tasks.PeriodicJob` will produce
        tasks according to their schedule.
        '''


class TaskQueue(pulsar.Application):
    '''A :class:`pulsar.apps.Application` for consuming
task.Tasks and managing scheduling of tasks via a
:class:`scheduler.Scheduler`.'''
    backend = None
    '''The :ref:`TaskBackend <apps-taskqueue-backend>` for this task queue.
    This picklable attribute is available once the :class:`TaskQueue` has
    started (that is when the :meth:`monitor_start` method is invoked by the
    :class:`pulsar.apps.ApplicationMonitor` running it).'''
    name = 'tasks'
    cfg = pulsar.Config(apps=('tasks',), timeout=600)

    def request_instance(self, request):
        return self.scheduler.get_task(request)
    
    def monitor_start(self, monitor):
        '''Starts running the task queue in ``monitor``.
        
        It calles the :attr:`pulsar.Application.callable` (if available) and
        create the :attr:`backend`.'''
        if self.callable:
            self.callable()
        self.backend = TaskBackend.make(
                                self.cfg.task_backend,
                                name=self.name,
                                task_paths=self.cfg.task_paths,
                                schedule_periodic=self.cfg.schedule_periodic,
                                max_tasks=self.cfg.max_requests,
                                backlog=self.cfg.concurrent_tasks)
        
    def monitor_task(self, monitor):
        '''Override the :meth:`pulsar.apps.Application.monitor_task` callback
to check if the :attr:`scheduler` needs to perform a new run.'''
        super(TaskQueue, self).monitor_task(monitor)
        if self.backend and monitor.is_running():
            if self.backend.next_run <= datetime.now():
                self.backend.tick()

    def worker_start(self, worker):
        self.backend.start(worker)
        
    def worker_stopping(self, worker):
        self.backend.close(worker)
        
    def actorparams(self, monitor, params):
        # Make sure we invoke super function so that we get the distributed
        # task queue
        params = super(TaskQueue, self).actorparams(monitor, params)
        # workers do not schedule periodic tasks
        params['app'].cfg.set('schedule_periodic', False)
        return params
    
    def worker_info(self, worker, data):
        be = self.backend
        tasks = {'concurrent': list(be.concurrent_tasks),
                 'processed': be.processed}
        data['tasks'] = tasks
        return data
     
    
@command()
def next_scheduled(request, jobnames=None):
    actor = request.actor
    return actor.app.backend.next_scheduled(jobnames)