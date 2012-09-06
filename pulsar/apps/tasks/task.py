'''\
A task scheduler application with HTTP-RPC hooks
'''
from datetime import datetime
import logging
import traceback
from io import StringIO

from pulsar.utils.httpurl import itervalues
from pulsar import maybe_async, as_failure, is_async, is_failure, send

from .models import registry
from .exceptions import *
from .states import *


__all__ = ['Task','TaskInMemory','TaskConsumer','nice_task_message']


class TaskConsumer(object):
    '''A context manager for consuming tasks. This is used by the
:class:`Scheduler` in a ``with`` block to correctly handle exceptions
and optional logging.

.. attribute:: task

    the :class:`Task` being consumed.

.. attribute:: job

    the :class:`Job` which generated the :attr:`task`.

.. attribute:: worker

    the :class:`pulsar.apps.Worker` running the process.
'''
    def __init__(self, task, worker, job):
        self.worker = worker
        self.job = job
        self.task = task


class Task(object):
    '''Interface for tasks which are produced by :class:`Job`.

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

    def consumer(self, worker, job):
        '''Return the task context manager for execution.'''
        return TaskConsumer(self, worker, job)

    def start(self, worker):
        '''Called by the :class:`pulsar.Worker` *worker* when the task
start its execution. If no timeout has occured the task will switch to
a ``STARTED`` :attr:`Task.status` and invoke the :meth:`on_start`
callback.'''
        job = registry[self.name]
        try:
            if self.maybe_revoked():
                yield self.on_timeout(worker)
            else:
                self.status = STARTED
                self.time_start = datetime.now()
                yield self.on_start(worker)
                consumer = TaskConsumer(self, worker, job)
                result = maybe_async(job(consumer, *self.args, **self.kwargs))
                if is_async(result):
                    yield result
                    result = maybe_async(result)
                self.result = result
        except Exception as e:
            self.result = as_failure(e)
        finally:
            yield self.finish(worker, result=self.result)

    def finish(self, worker, result):
        '''called when finishing the task.'''
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
            return self.on_finish(worker)

    def to_queue(self, schedulter=None):
        '''The task has been received by the scheduler. If its status
is PENDING swicth to RECEIVED, save the task and return it. Otherwise
returns nothing.'''
        self._toqueue = False
        if self.status == PENDING:
            self.status = RECEIVED
            self._toqueue = True
            self.on_received(schedulter)
        return self

    def needs_queuing(self):
        '''called after calling :meth:`to_queue`, it return ``True`` if the
task needs to be queued.'''
        return self.__dict__.pop('_toqueue', False)

    def done(self):
        '''Return ``True`` if the task has its staus in READY_STATES'''
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

    def serialize_for_queue(self):
        return self

    def ack(self):
        return self

    ############################################################################
    ##    FACTORY METHODS
    ############################################################################
    @classmethod
    def get_task(cls, scheduler, id):
        '''Given a task *id* it retrieves a task instance or ``None`` if
not available.'''
        raise NotImplementedError()
    
    @classmethod
    def save_task(cls, scheduler, task):
        raise NotImplementedError()
        
    @classmethod
    def delete_tasks(cls, scheduler, task):
        raise NotImplementedError()

    ############################################################################
    # CALLBACKS
    ############################################################################

    def on_created(self, scheduler=None):
        '''A :ref:`task callback <tasks-callbacks>` when the task has
has been created.

:parameter scheduler: the scheduler which created the task.
'''
        pass

    def on_received(self, scheduler=None):
        '''A :ref:`task callback <tasks-callbacks>` when the task has
has been received by the scheduler.'''
        pass

    def on_start(self, worker=None):
        '''A :ref:`task callback <tasks-callbacks>` when the task starts
its execution'''
        pass

    def on_timeout(self, worker=None):
        '''A :ref:`task callback <tasks-callbacks>` when the task is expired'''
        pass

    def on_finish(self, worker=None):
        '''A :ref:`task callback <tasks-callbacks>` when the task finish
its execution'''
        pass

    def close(self):
        pass

    def emit_log(self, record):
        '''Implement the task logging emit method. By default it does nothing.
It can be reimplemented to do something with the log record.'''
        pass


class TaskInMemory(Task):
    '''An in memory implementation of a Task.'''
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

    def __str__(self):
        return '{0}({1})'.format(self.name,self.id)

    def on_received(self, scheduler=None):
        # Called by the scheduler
        self.save_task(scheduler, self)

    def on_start(self, worker=None):
        if worker:
            return send(worker.monitor, 'save_task', self)

    def on_timeout(self, worker=None):
        if worker:
            return send(worker.monitor, 'save_task', self)

    def on_finish(self, worker=None):
        if worker:
            return send(worker.monitor, 'save_task', self)

    @classmethod
    def task_container(cls, scheduler):
        if not hasattr(scheduler, '_TASKS'):
            scheduler._TASKS = {}
        return scheduler._TASKS
    
    @classmethod
    def save_task(cls, scheduler, task):
        TASKS = cls.task_container(scheduler)
        if task.id in TASKS:
            t = TASKS[task.id]
            if t.status_code < task.status_code:
                # we don't save here. Could by concurrency lags
                return
        TASKS[task.id] = task

    @classmethod
    def delete_tasks(cls, scheduler, ids=None):
        TASKS = cls.task_container(scheduler)
        if ids:
            for id in ids:
                TASKS.pop(id, None)
        else:
            TASKS.clear()
        
    @classmethod
    def get_task(cls, scheduler, id):
        return cls.task_container(scheduler).get(id)


def nice_task_message(req, smart_time=None):
    smart_time = smart_time or format_time
    status = req['status'].lower()
    user = req.get('user',None)
    ti = req.get('time_start',req.get('time_executed',None))
    name = '{0} ({1}) '.format(req['name'],req['id'][:8])
    msg = '{0} {1} at {2}'.format(name,status,smart_time(ti))
    if user:
        msg = '{0} by {1}'.format(msg,user)
    return msg