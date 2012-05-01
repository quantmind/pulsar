'''\
A task scheduler application with HTTP-RPC hooks
'''
from datetime import datetime
import logging
import traceback

from pulsar.utils.py2py3 import StringIO, itervalues
from pulsar import make_async, as_failure

from .exceptions import *
from .states import *


__all__ = ['Task','TaskInMemory','TaskConsumer','nice_task_message']


class TaskLoggingHandler(logging.Handler):

    def __init__(self, job, task):
        self.job = job
        self.task = task
        super(TaskLoggingHandler,self).__init__(job.loglevel)
        
    def emit(self, record):
        msg = self.format_msg(record)
        try:
            self.task.emit_log(record)
        except:
            pass
    

class TaskConsumer(object):
    '''A context manager for consuming tasks. This is used by the
:class:`Scheduler` in a ``with`` block to correctly handle exceptions
and optional logging.

.. attribute:: task

    the :class:`Task` being consumed.
    
.. attribute:: job

    the :class:`Job` which generated the :attr:`task`.
    
.. attribute:: queue

    the :class:`TaskQueue` application.
    
.. attribute:: worker

    the :class:`pulsar.apps.Worker` running the process.
'''
    def __init__(self, task, queue, worker, job):
        self.queue = queue
        self.worker = worker
        self.job = job
        self.task = task
        if job.loglevel is not None:
            self.handler = TaskLoggingHandler(job, task)
            formatter = job.logformatter
            if not formatter:
                h = logging.getLogger().handlers
                if h:
                    formatter = h[0].formatter
            if formatter:
                self.handler.setFormatter(formatter)
        else:
            self.handler = None
    
    def __enter__(self):
        if self.handler is not None:
            self.job.logger.addHandler(self.handler)
        return self
    
    def __exit__(self, type, value, trace):
        result = value
        if type:
            self.job.logger.critical(
                        'Critical while processing task {0}'.format(self.task),
                        exc_info = (type, value, trace))
            result = as_failure((type, value, trace))
        if self.handler is not None:
            self.job.logger.removeHandler(self.handler)
        if type:
            return make_async(self.task.finish(self.worker,
                                               exception = value,
                                               result = result))\
                    .add_callback(lambda r : result)
        else:
            return result


class Task(object):
    '''A Task interface.

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
    
    A boolean indicating whether a timeout has occurred.
    
.. attribute:: from_task

    Optional :attr:`Task.id` for the :class:`Task` which queued
    this :class:`Task`. This is a usuful for monitoring the creation
    of tasks within other tasks.
'''
    from_task = None
    stack_trace = None
    
    @property
    def state(self):
        '''Integer indicating :attr:`status` precedence.
Lower number higher precedence.'''
        return PRECEDENCE_MAPPING.get(self.status,UNKNOWN_STATE)
    
    def consumer(self, queue, worker, job):
        '''Return the task context manager for execution.'''
        return TaskConsumer(self, queue, worker, job)
        
    def start(self, worker):
        '''Called by the :class:`pulsar.Worker` *worker* when the task
start its execution. If no timeout has occured the task will switch to
a ``STARTED`` :attr:`Task.status` and invoke the :meth:`on_start`
callback.'''
        self.time_start = datetime.now()
        if self.maybe_revoked():
            self.on_timeout(worker)
            raise TaskTimeout()
        else:
            self.status = STARTED
            return self.on_start(worker)
        
    def finish(self, worker, exception = None, result = None):
        '''called when finishing the task.'''
        if not self.time_end:
            self.time_end = datetime.now()
            if exception:
                self.status = getattr(exception, 'status', FAILURE)
                self.result = str(result) if result else str(exception)
            else:
                self.status = SUCCESS
                self.result = result
            self.on_finish(worker)
            worker.app.broadcast('finish', self)
        return self
        
    def to_queue(self, schedulter = None):
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
        return self.__dict__.pop('_toqueue',False)
        
    def done(self):
        '''Return ``True`` if the task has its staus in READY_STATES'''
        return self.status in READY_STATES
    
    def maybe_revoked(self):
        '''Try to revoke a task. If succesful return the expiry.

:rtype: ``None`` or a DateTime instance'''
        if self.state > PRECEDENCE_MAPPING[STARTED] and self.expiry:
            tm = datetime.now()
            if tm > self.expiry:
                self.status = REVOKED
                self.timeout = tm
                return tm
        
    def execute2start(self):
        if self.time_start:
            return self.time_start - self.time_executed
        
    def execute2end(self):
        if self.time_end:
            return self.time_end - self.time_executed
        
    def duration(self):
        if self.time_end:
            return self.time_end - self.time_start  

    def tojson(self):
        '''Convert the task instance into a JSON-serializable dictionary.'''
        return self.__dict__.copy()
    
    def on_same_id(self):
        self.delete()
    
    def serialize_for_queue(self):
        return self
    
    def ack(self):
        return self
    
    ############################################################################
    ##    FACTORY METHODS
    
    @classmethod
    def from_queue(cls, task):
        return task
    
    @classmethod
    def get_task(cls, id, remove = False):
        '''Given a task *id* it retrieves a task instance or ``None`` if
not available.'''
        raise NotImplementedError()
    
    @classmethod
    def check_unready_tasks(cls):
        '''Check for expiries in all tasks in an un-ready state.'''
        raise NotImplementedError()
    
    ############################################################################
    # CALLBACKS
    ############################################################################
    
    def on_created(self, scheduler = None):
        '''A :ref:`task callback <tasks-callbacks>` when the task has
has been created.

:parameter scheduler: the scheduler which created the task.
'''
        pass
    
    def on_received(self, scheduler = None):
        '''A :ref:`task callback <tasks-callbacks>` when the task has
has been received by the scheduler.'''
        pass
        
    def on_start(self, worker = None):
        '''A :ref:`task callback <tasks-callbacks>` when the task starts
its execution'''
        pass
    
    def on_timeout(self, worker=None):
        '''A :ref:`task callback <tasks-callbacks>` when the task is expired'''
        pass

    def on_finish(self, worker = None):
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
    '''An in memory implementation of a Task'''
    _TASKS = {}
    time_start = None
    time_end = None
    stack_trace = None
    result = None
    timeout = False
    
    def __init__(self, id = None, name = None, time_executed = None,
                 expiry = None, args = None, kwargs = None, ack = None,
                 status = None, from_task = None, **params):
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

    def on_received(self,worker=None):
        # Called by the scheduler
        self.__class__.save_task(self)
    
    def on_start(self, worker=None):
        if worker:
            return worker.monitor.send(worker,'save_task',self)
        
    def on_timeout(self, worker=None):
        if worker:
            return worker.monitor.send(worker,'save_task',self)
                    
    def on_finish(self,worker=None):
        if worker:
            return worker.monitor.send(worker, 'save_task', self)
        
    def delete(self):
        self._TASKS.pop(self.id,None)
        
    @classmethod
    def save_task(cls, task):
        if task.id in cls._TASKS:
            t = cls._TASKS[task.id]
            if t.state < task.state:
                # we don't save here. Could by concurrency lags
                return
        cls._TASKS[task.id] = task
    
    @classmethod
    def get_task(cls, id, remove=False):
        task = cls._TASKS.get(id, None)
        if remove and task:
            if task.done():
                cls._TASKS.pop(id)
        return task
    
    @classmethod
    def check_unready_tasks(cls):
        tasks = cls._TASKS
        for task in list(itervalues(tasks)):
            task.maybe_revoked()
            if task.done():
                tasks.pop(task.id,None)
            

def nice_task_message(req, smart_time = None):
    smart_time = smart_time or format_time
    status = req['status'].lower()
    user = req.get('user',None)
    ti = req.get('time_start',req.get('time_executed',None))
    name = '{0} ({1}) '.format(req['name'],req['id'][:8])
    msg = '{0} {1} at {2}'.format(name,status,smart_time(ti))
    if user:
        msg = '{0} by {1}'.format(msg,user)
    return msg