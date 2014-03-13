'''To get started, follow these guidelines:

* Create a script which runs your application, in the
  :ref:`taskqueue tutorial <tutorials-taskqueue>` the script is called
  ``manage.py``.
* Create the modules where :ref:`jobs <app-taskqueue-job>` are implemented. It
  can be a directory containing several submodules as explained in the
  :ref:`task paths parameter <app-tasks_path>`.
* Run your script, sit back and relax.

.. _app-taskqueue-config:

Configuration
~~~~~~~~~~~~~~~~
A :class:`TaskQueue` accepts several configuration parameters on top of the
standard :ref:`application settings <settings>`:

.. _app-tasks_path:

* The :ref:`task_paths <setting-task_paths>` parameter specifies
  a list of python paths where to collect :class:`.Job` classes::

      task_paths = ['myjobs','another.moduledir.*']

  The ``*`` at the end of the second module indicates to collect
  :class:`.Job` from all submodules of ``another.moduledir``.

* The :ref:`schedule_periodic <setting-schedule_periodic>` flag indicates
  if the :class:`TaskQueue` can schedule :class:`.PeriodicJob`. Usually,
  only one running :class:`TaskQueue` application is responsible for
  scheduling tasks.

  It can be specified in the command line via the
  ``--schedule-periodic`` flag.

  Default: ``False``.

* The :ref:`task_backend <setting-task_backend>` parameter is a url
  type string which specifies the :ref:`task backend <apps-taskqueue-backend>`
  to use.

  It can be specified in the command line via the
  ``--task-backend ...`` option.

  Default: ``local://``.

* The :ref:`concurrent_tasks <setting-concurrent_tasks>` parameter controls
  the maximum number of concurrent tasks for a given task worker.
  This parameter is important when tasks are asynchronous, that is when
  they perform some sort of I/O and the :ref:`job callable <job-callable>`
  returns and :ref:`asynchronous component <tutorials-coroutine>`.

  It can be specified in the command line via the
  ``--concurrent-tasks ...`` option.

  Default: ``5``.

.. _celery: http://celeryproject.org/
'''
import time

import pulsar
from pulsar import command
from pulsar.utils.config import section_docs
from pulsar.apps.data import create_store
from pulsar.apps.data.stores import start_store
from pulsar.apps.ds import DEFAULT_PULSAR_STORE_ADDRESS

from .models import *
from .backend import *
from .rpc import *
from .states import *


DEFAULT_TASK_BACKEND = 'pulsar://%s/1' % DEFAULT_PULSAR_STORE_ADDRESS

section_docs['Task Consumer'] = '''
This section covers configuration parameters used by CPU bound type
applications such as the :ref:`distributed task queue <apps-taskqueue>` and
the :ref:`test suite <apps-test>`.'''


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
    default = ""
    meta = 'CONNECTION_STRING'
    desc = '''\
        Connection string for the backend storing tasks.

        If the value is not available (default) it uses as fallback the
        :ref:`data_store <setting-data_store>` value. If still not
        set, it uses the ``%s`` value.
        ''' % DEFAULT_TASK_BACKEND


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

        If enabled, :class:`.PeriodicJob` will produce
        tasks according to their schedule.
        '''


class TaskQueue(pulsar.Application):
    '''A pulsar :class:`.Application` for consuming :class:`.Task`.

    This application can also schedule periodic tasks when the
    :ref:`schedule_periodic <setting-schedule_periodic>` flag is ``True``.
    '''
    backend = None
    '''The :class:`.TaskBackend` for this task queue.

    Available once the :class:`.TaskQueue` has started.
    '''
    name = 'tasks'
    cfg = pulsar.Config(apps=('tasks',), timeout=600)

    def monitor_start(self, monitor):
        '''Starts running the task queue in ``monitor``.

        It calls the :attr:`.Application.callable` (if available)
        and create the :attr:`~.TaskQueue.backend`.
        '''
        if self.cfg.callable:
            self.cfg.callable()
        connection_string = (self.cfg.task_backend or self.cfg.data_store or
                             DEFAULT_TASK_BACKEND)
        store = yield start_store(connection_string, loop=monitor._loop)
        self.get_backend(store)

    def monitor_task(self, monitor):
        '''Override the :meth:`~.Application.monitor_task` callback.

        Check if the :attr:`~.TaskQueue.backend` needs to schedule new tasks.
        '''
        if self.backend and monitor.is_running():
            if self.backend.next_run <= time.time():
                self.backend.tick()

    def monitor_stopping(self, monitor, exc=None):
        if self.backend:
            self.backend.close()

    def worker_start(self, worker, exc=None):
        if not exc:
            self.get_backend().start(worker)

    def worker_stopping(self, worker, exc=None):
        if self.backend:
            self.backend.close()

    def actorparams(self, monitor, params):
        # makes sure workers are only consuming tasks, not scheduling.
        cfg = params['cfg']
        cfg.set('schedule_periodic', False)

    def worker_info(self, worker, info=None):
        be = self.backend
        if be:
            tasks = {'concurrent': list(be.concurrent_tasks),
                     'processed': be.processed}
            info['tasks'] = tasks

    def get_backend(self, store=None):
        if self.backend is None:
            if store is None:
                store = create_store(self.cfg.task_backend)
            else:
                self.cfg.set('task_backend', store.dns)
            task_backend = task_backends.get(store.name)
            if not task_backend:
                raise pulsar.ImproperlyConfigured(
                    'Task backend for %s not available' % store.name)
            self.backend = task_backend(
                store,
                logger=self.logger,
                name=self.name,
                task_paths=self.cfg.task_paths,
                schedule_periodic=self.cfg.schedule_periodic,
                max_tasks=self.cfg.max_requests,
                backlog=self.cfg.concurrent_tasks)
            self.logger.debug('created %s', self.backend)
        return self.backend


@command()
def next_scheduled(request, jobnames=None):
    actor = request.actor
    return actor.app.backend.next_scheduled(jobnames)
