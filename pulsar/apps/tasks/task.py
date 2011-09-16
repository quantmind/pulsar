'''\
A task scheduler application with HTTP-RPC hooks
'''
from datetime import datetime
import logging

from pulsar.utils.py2py3 import StringIO
from .exceptions import *
from .states import *


__all__ = ['Task','TaskInMemory']
    

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
            self.task.on_finish(self.worker, exception = value)
        return value


class Task(object):
    '''A Task interface.

.. attribute: name
    
    Job name
    
.. attribute: status

    Current status of task
    
.. attribute: time_executed

    date time when the task was executed
    
.. attribute: time_start

    date-time when the task calculation has started.
    
.. attribute: time_end

    date-time when the task has finished.
    
.. attribute: expiry

    optional date-time indicating when the task should expire.
    
.. attribute: timeout
    
    A boolean indicating whether a timeout has occurred
'''
    def consumer(self, queue, worker, job):
        '''Return the task context manager for execution.'''
        return TaskConsumer(self, queue, worker, job)
        
    def on_start(self,worker):
        '''Called by worker when the task start its execution.'''
        self.time_start = datetime.now()
        timeout = self.revoked()
        self.timeout = timeout
        if timeout:
            raise TaskTimeout()
        else:
            self.status = STARTED
            self.save()
            self._on_start(worker)
            return True
        
    def on_finish(self, worker, exception = None, result = None):
        '''called when finishing the task.'''
        if not self.time_end:
            self.time_end = datetime.now()
            if exception:
                self.status = getattr(exception,'status',FAILURE)
                self.result = str(exception)
            else:
                self.status = SUCCESS
                self.result = result
            self.save()
            self._on_finish(worker)
        
    def to_queue(self):
        if self.status == PENDING:
            self.status = RECEIVED
            return self.save()
        
    def done(self):
        if self.time_end:
            return True
        else:
            return self.revoked()
        
    def revoked(self):
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

    def tojson_dict(self):
        return self.__dict__.copy()
    
    def on_same_id(self):
        self.delete()
    
    @classmethod
    def get_task(cls, id, remove = False):
        raise NotImplementedError
    
    def ack(self):
        return self
    
    def _on_start(self, worker):
        pass
    
    def _on_finish(self, worker):
        pass


class TaskInMemory(Task):
    '''An in memory implementation of a Task'''
    _TASKS = {}
    _TASKS_DONE = {}
    time_start = None
    time_end = None
    stack_trace = None
    result = None
    timeout = False
    
    def __init__(self, id = None, name = None, time_executed = None,
                 expiry = None, args = None, kwargs = None, ack = None,
                 status = None):
        self.id = id
        self.name = name
        self.time_executed = time_executed
        self.expiry = expiry
        self.args = args
        self.kwargs = kwargs
        self.status = status

    def _on_finish(self,worker=None):
        if worker:
            worker.monitor.send(worker.aid, ((self,),{}),
                                name = 'task_finished')

    def save(self):
        self._TASKS[self.id] = self
        return self
        
    def delete(self):
        self._TASKS.pop(self.id,None)
        
    @classmethod
    def get_task(cls,id,remove=False):
        task = cls._TASKS.get(id,None)
        if remove and task:
            if task.done():
                cls._TASKS.pop(id)
        return task
