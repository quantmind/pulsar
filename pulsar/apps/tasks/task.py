'''\
A task scheduler application with HTTP-RPC hooks
'''
from datetime import datetime
import logging

from pulsar.utils.py2py3 import StringIO
from pulsar import make_async

from .exceptions import *
from .states import *


__all__ = ['Task','TaskInMemory','nice_task_message']
    

class TaskConsumer(object):
    '''A context manager for consuming tasks'''
    def __init__(self, task, queue, worker, job):
        self.queue = queue
        self.worker = worker
        self.job = job
        self.task = task
        self.handler = logging.StreamHandler(StringIO())
        formatter = job.logformatter
        if not formatter:
            h = logging.getLogger().handlers
            if h:
                formatter = h[0].formatter
        if formatter:
            self.handler.setFormatter(formatter)
        
    def get_logs(self):
        self.job.logger.removeHandler(self.handler)
        return self.handler.stream.getvalue()
    
    def __enter__(self):
        self.job.logger.addHandler(self.handler)
        return self
    
    def __exit__(self, type, value, traceback):
        if type:
            self.job.logger.critical('', exc_info = (type, value, traceback))
        self.task.logs = self.get_logs()
        if type:
            return make_async(self.task.finish(self.worker, exception = value))\
                    .add_callback(lambda r : value)
        else:
            return value


class Task(object):
    '''A Task interface.

.. attribute:: name
    
    :class:`Job` name
    
.. attribute:: status

    The current :ref:`status string <task-state>` of task
    
.. attribute:: time_executed

    date time when the task was executed
    
.. attribute:: time_start

    date-time when the task calculation has started.
    
.. attribute:: time_end

    date-time when the task has finished.
    
.. attribute:: expiry

    optional date-time indicating when the task should expire.
    
.. attribute:: timeout
    
    A boolean indicating whether a timeout has occurred
'''
    @property
    def state(self):
        '''Integer indicating :attr:`status` precedence.
Lower number higher precedence.'''
        return PRECEDENCE_MAPPING.get(self.status,UNKNOWN_STATE)
    
    def consumer(self, queue, worker, job):
        '''Return the task context manager for execution.'''
        return TaskConsumer(self, queue, worker, job)
        
    def start(self,worker):
        '''Called by worker when the task start its execution.'''
        self.time_start = datetime.now()
        timeout = self.revoked()
        self.timeout = timeout
        if timeout:
            raise TaskTimeout()
        else:
            self.status = STARTED
            return self.on_start(worker)
        
    def finish(self, worker, exception = None, result = None):
        '''called when finishing the task.'''
        if not self.time_end:
            self.time_end = datetime.now()
            if exception:
                self.status = getattr(exception,'status',FAILURE)
                self.result = str(exception)
            else:
                self.status = SUCCESS
                self.result = result
            self.on_finish(worker)
        return self
        
    def to_queue(self):
        '''The task has been received by the scheduler. If its status
is PENDING swicth to RECEIVED, save the task and return it. Otherwise
returns nothing.'''
        self._toqueue = False
        if self.status == PENDING:
            self.status = RECEIVED
            self._toqueue = True
            self.on_received()
        return self
        
    def needs_queuing(self):
        '''called after calling :meth:`to_queue`, it return ``True`` if the
task needs to be queued.'''
        return self.__dict__.pop('_toqueue',False)
        
    def done(self):
        '''Return ``True`` if the task has finished its execution.'''
        if self.time_end:
            return True
        
    def revoked(self):
        '''Attempt to revoke the task, if the task is not in
:attr:`READY_STATES`. It returns a timestamp if the revoke was successful.'''
        if self.expiry:
            tm = datetime.now()
            if tm > self.expiry:
                return tm
        return None
    
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
    
    @classmethod
    def get_task(cls, id, remove = False):
        '''Given a task *id* it retrieves a task instance or ``None`` if
not available.'''
        raise NotImplementedError
    
    def ack(self):
        return self
    
    ############################################################################
    # CALLBACKS
    ############################################################################
    
    def on_created(self):
        '''A :ref:`task callback <tasks-callbacks>` when the task has
has been created.'''
        pass
    
    def on_received(self, worker = None):
        '''A :ref:`task callback <tasks-callbacks>` when the task has
has been received by the scheduler.'''
        pass
        
    def on_start(self, worker = None):
        '''A :ref:`task callback <tasks-callbacks>` when the task starts
its execution'''
        pass
    
    def on_finish(self, worker = None):
        '''A :ref:`task callback <tasks-callbacks>` when the task finish
its execution'''
        pass

    def close(self):
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
                 status = None, **params):
        self.id = id
        self.name = name
        self.time_executed = time_executed
        self.expiry = expiry
        self.args = args
        self.kwargs = kwargs
        self.status = status
        self.params = params
        
    def __str__(self):
        return '{0}({1})'.format(self.name,self.id)

    def on_received(self,worker=None):
        # Called by the scheduler
        self.__class__.save_task(self)
    
    def on_start(self, worker=None):
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
    def get_task(cls,id,remove=False):
        task = cls._TASKS.get(id,None)
        if remove and task:
            if task.done():
                cls._TASKS.pop(id)
        return task


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