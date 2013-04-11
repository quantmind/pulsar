'''\
A :class:`Task` is created every time a
:class:`pulsar.apps.tasks.scheduler.Scheduler` invokes its ``run`` or
``queue_task`` method.

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

* ``PENDING`` A task in the task queue waiting for execution.
* ``RETRY`` A task is retrying calculation.
* ``STARTED`` task execution has started.
* ``REVOKED`` the task execution has been revoked. One possible reason could be
  the task has timed out.
* ``UNKNOWN`` task execution is unknown.
* ``FAILURE`` task execution has finished with failure.
* ``SUCCESS`` task execution has finished with success.


.. attribute:: FULL_RUN_STATES

    The set of states for which a :class:`Task` has run:
    ``FAILURE`` and ``SUCCESS``

.. _task-state-ready:

.. attribute:: READY_STATES

    The set of states for which a :class:`Task` has finished:
    ``REVOKED``, ``FAILURE`` and ``SUCCESS``


.. _tasks-interface:

Task Interface
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Task
   :members:
   :member-order: bysource
   
TaskConsumer
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: TaskConsumer
   :members:
   :member-order: bysource
'''
from datetime import datetime
import logging
import traceback
from io import StringIO

from pulsar.utils.pep import itervalues, iteritems
from pulsar import async, maybe_failure, get_actor, is_failure, send, command

from .models import registry
from .exceptions import *
from .states import *


__all__ = ['Task', 'TaskInMemory', 'TaskConsumer', 'nice_task_message']


class TaskConsumer(object):
    '''A context manager for consuming tasks.

.. attribute:: task

    the :class:`Task` being consumed.

.. attribute:: job

    the :class:`Job` which generated the :attr:`task`.

.. attribute:: worker

    the :class:`pulsar.apps.Worker` running the process.
    
.. attribute:: scheduler

    give access to the :class:`Scheduler`.
'''
    def __init__(self, task, job):
        self.worker = get_actor()
        self.job = job
        self.task = task
        
    @property
    def scheduler(self):
        return self.worker.app.scheduler
    

class Task(object):
    '''Interface for tasks which are produced by
:ref:`jobs or periodic jobs <apps-taskqueue-job>`.

.. attribute:: id

    :class:`Task` unique id.

.. attribute:: name

    :class:`Job` name.

.. attribute:: status

    The current :ref:`status string <task-state>` of task.

.. attribute:: time_executed

    date time when the task was executed.

.. attribute:: time_start

    date-time when the task calculation has started.

.. attribute:: time_end

    date-time when the task has finished.

.. attribute:: expiry

    optional date-time indicating when the task should expire.

.. attribute:: timeout

    A datetime or ``None`` indicating whether a timeout has occurred.

.. attribute:: from_task

    Optional :attr:`Task.id` for the :class:`Task` which queued
    this :class:`Task`. This is a usuful for monitoring the creation
    of tasks within other tasks.
'''
    from_task = None
    stack_trace = None

    @property
    def status_code(self):
        '''Integer indicating :attr:`status` precedence.
Lower number higher precedence.'''
        return PRECEDENCE_MAPPING.get(self.status, UNKNOWN_STATE)

    @async()
    def start(self):
        '''Starts execution of this :class:`Task`. If no timeout has
occurred the task will switch to a ``STARTED`` :attr:`Task.status` and
invoke the :meth:`on_start` callback.'''
        job = registry[self.name]
        result = None
        try:
            if self.maybe_revoked():
                yield self.on_timeout()
            else:
                self.status = STARTED
                self.time_start = datetime.now()
                yield self.on_start()
                consumer = TaskConsumer(self, job)
                result = yield job(consumer, *self.args, **self.kwargs)
        except Exception as e:
            result = maybe_failure(e)
        finally:
            yield self.finish(result)

    def finish(self, result):
        '''Called when the task has finished and a ``result`` is ready.
It sets the :attr:`time_end` attribute if not already set
(in case the :class:`Task` was revoked) and
determined if it was succesful or a failure. Once done it invokes
the :ref:`task callback <tasks-callbacks>` :meth:`on_finish`.'''
        if not self.time_end:
            self.time_end = datetime.now()
            if is_failure(result):
                result.log()
                exception = result.trace[1]
                self.status = getattr(exception, 'status', FAILURE)
                self.result = str(result) if result else str(exception)
            # If the status is STARTED this is a succesful task
            elif self.status == STARTED:
                self.status = SUCCESS
                self.result = result
        return self.on_finish()

    def done(self):
        '''Return ``True`` if the :class:`Task` has finshed
(its status is one of :ref:`READY_STATES <task-state>`).'''
        return self.status in READY_STATES

    def maybe_revoked(self):
        '''Try to revoke a task. It returns the :attr:`timeout` which is
different from ``None`` only when the :class:`Task` has been revoked.

:rtype: ``None`` or a DateTime.'''
        if not self.timeout:
            if self.status_code > PRECEDENCE_MAPPING[STARTED] and self.expiry:
                tm = datetime.now()
                if tm > self.expiry:
                    self.status = REVOKED
                    self.timeout = tm
        return self.timeout

    def execute2start(self):
        if self.time_start:
            return self.time_start - self.time_executed

    def execute2end(self):
        if self.time_end:
            return self.time_end - self.time_executed

    def duration(self):
        '''The :class:`Task` duration. Only available if the task status is in
:attr:`FULL_RUN_STATES`.'''
        if self.time_end and self.time_start:
            return self.time_end - self.time_start

    def tojson(self):
        '''Convert the task instance into a JSON-serializable dictionary.'''
        return self.__dict__.copy()

    def ack(self):
        return self

    ############################################################################
    # ABSTRACT METHODS
    ############################################################################

    def save(self):
        '''Save the :class:`Task` into its backend.'''
        raise NotImplementedError

    def on_start(self):
        '''A :ref:`task callback <tasks-callbacks>` when the task starts
its execution'''
        pass

    def on_timeout(self):
        '''A :ref:`task callback <tasks-callbacks>` when the task is expired'''
        pass

    def on_finish(self):
        '''A :ref:`task callback <tasks-callbacks>` when the task finish
its execution'''
        pass

    def close(self):
        pass

    def emit_log(self, record):
        '''Implement the task logging emit method. By default it does nothing.
It can be reimplemented to do something with the log record.'''
        pass
    
    ############################################################################
    ##    FACTORY METHODS
    ############################################################################
    @classmethod
    def get_task(cls, scheduler, id):
        '''Given a task *id* it retrieves a task instance or ``None`` if
not available.'''
        raise NotImplementedError()
    
    @classmethod
    def get_tasks(cls, scheduler, **filters):
        '''Given *filters* it retrieves task instances which satisfy the
filter criteria.'''
        raise NotImplementedError()
    
    @classmethod
    def save_task(cls, scheduler, task):
        raise NotImplementedError()
        
    @classmethod
    def delete_tasks(cls, scheduler, task):
        raise NotImplementedError()
    
    @classmethod
    def wait_for_task(cls, scheduler, id, timeout):
        '''Wait for a task to have finished.'''
        raise NotImplementedError


def format_time(dt):
    return dt.isoformat() if dt else '?'

def nice_task_message(req, smart_time=None):
    smart_time = smart_time or format_time
    status = req['status'].lower()
    user = req.get('user')
    ti = req.get('time_start', req.get('time_executed'))
    name = '%s (%s) ' % (req['name'], req['id'][:8])
    msg = '%s %s at %s' % (name, status, smart_time(ti))
    return '%s by %s' % (msg, user) if user else msg


################################################################################
##    A Pulsar Task Implementation

class TaskInMemory(Task):
    '''An in memory implementation of a :class:`Task`.'''
    time_start = None
    time_end = None
    stack_trace = None
    result = None
    timeout = False

    def __init__(self, id=None, name=None, time_executed=None,
                 expiry=None, args=None, kwargs=None, ack=None,
                 status=None, from_task=None, **params):
        self.id = id
        self.name = name
        self.time_executed = time_executed
        self.expiry = expiry
        self.args = args
        self.kwargs = kwargs
        self.status = status
        self.from_task = from_task
        self.params = params

    def __repr__(self):
        return '%s(%s)' % (self.name,self.id)
    __str__ = __repr__

    def save(self):
        return send('arbiter', 'save_task', self)

    def on_start(self):
        return send('arbiter', 'save_task', self)

    def on_timeout(self):
        return send('arbiter', 'save_task', self)

    def on_finish(self):
        return send('arbiter', 'save_task', self)

    @classmethod
    def delete_tasks(cls, ids=None):
        return send('arbiter', 'delete_tasks', ids)
        
    @classmethod
    def get_task(cls, id):
        return send('arbiter', 'get_task', id)
    
    @classmethod
    def get_tasks(cls, **filters):
        return send('arbiter', 'get_tasks', **filters)


#################################################    TASKQUEUE COMMANDS
@command()
def save_task(request, task):
    TASKS = _get_tasks(request.actor)
    if task.id in TASKS:
        t = TASKS[task.id]
        if t.status_code < task.status_code:
            # we don't save here.
            return t
    TASKS[task.id] = task
    return task

@command()
def delete_tasks(request, ids):
    TASKS = _get_tasks(request.actor)
    if ids:
        for id in ids:
            TASKS.pop(id, None)
    else:
        TASKS.clear()

@command()
def get_task(request, id):
    TASKS = _get_tasks(request.actor)
    return TASKS.get(id)

@command()
def get_tasks(request, **filters):
    TASKS = _get_tasks(request.actor)
    tasks = []
    if filters:
        fs = []
        for name, value in iteritems(filters):
            if not isinstance(value, (list, tuple)):
                value = (value,)
            fs.append((name, value))
        for t in itervalues(TASKS):
            for name, values in fs:
                value = getattr(t, name, None)
                if value in values:
                    tasks.append(t)
    return tasks

def _get_tasks(actor):
    tasks = actor.params.TASKS
    if tasks is None:
        actor.params.TASKS = tasks = {}
    return tasks