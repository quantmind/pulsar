import os
import time
import logging
from datetime import datetime

from unuk.contrib.tasks import registry
from unuk.utils.importer import import_modules

time2datetime = lambda ts : None if not ts else datetime.fromtimestamp(ts)

class InvalidTaskError(Exception):
    """The task has invalid data or is not properly constructed."""


class AlreadyExecutedError(Exception):
    """Tasks can only be executed once, as they might change
    world-wide state."""


def execute_in_process(task_name, task_id, task_modules, *args, **kwargs):
    import_modules(task_modules)
    logger = logging.getLogger('task.request.execute_in_process')
    logger.debug('Preparing to execute %s of %s' % (task_name,task_id))
    try:
        task = registry[task_name]
    except registry.NotRegistered, e:
        logger.error("Registry tasks: %s not available" % task_name)
        raise e
    else:
        return task(task_name, task_id, *args, **kwargs)


def unique_request_name(task, *args, **kwargs):
    code = task.name
    if args:
        code = '%s_%s' % (code,reduce(lambda c,n: '%s_%s' % (c,n),args))
    if kwargs:
        code = '%s%s' % (code,reduce(lambda c,n: '%s_%s=%s' % (c,n,kwargs[n]),sorted(kwargs),''))
    return code
    


class TaskEvent(object):
    '''Event associated with a :class:`TaskRequest` instance. This class is used internally by the task library.'''
    fired  = False
    '''Flag indicating if ``self`` has been already fired.'''
    result = None
    
    def __init__(self):
        self.callbacks = []
    
    def fire(self, result):
        if self.fired:
            raise ValueError("Event already called")
        self.fired = True
        self.result = result
        for callback in self.callbacks:
            callback(**self.result)
            
    def bind(self, callback):
        '''Bind a *callback* to ``self``. If the ``self`` has been already been fired,
the *callback* is immediately called, otherwise is appended to the callback list.'''
        if self.fired:
            callback(**self.result)
        else:
            self.callbacks.append(callback)


def requestinfo(r):
    info = TaskRequestInfo()
    info.__dict__ = r.copy()
    return info
    

class TaskRequestInfo(object):
    time_executed = None
    time_start    = None
    time_end      = None
    result        = None
    exception     = None
    timeout       = False
    
    def __init__(self, name = None, id = None, code = None, args = None, kwargs = None):
        self.name   = name
        self.id     = id
        self.code   = code
        self.args   = args
        self.kwargs = kwargs
        
    def _setstart(self):
        if self.time_start:
            raise AlreadyExecutedError(
                   "Task %s[%s] has already been executed" % (
                       self.name, self.id))
        self.time_start = time.time()
        
    def _setfinish(self, timeout = False):
        self.timeout   = timeout
        self.exception = timeout
        self.time_end  = time.time()
        
    def _setexecuted(self):
        if self.time_executed:
            raise AlreadyExecutedError(
                   "Task %s[%s] has already been executed" % (
                       self.name, self.id))
        self.time_executed = time.time()
        
    def todict(self, safe = False):
        d = self.__dict__.copy()
        d['time_start'] = time2datetime(d.get('time_start',None))
        d['time_executed'] = time2datetime(d.get('time_executed',None))
        d['time_end'] = time2datetime(d.get('time_end',None))
        return d
    
    def execute2start(self):
        if self.time_start:
            return self.time_start - self.time_executed
        
    def execute2end(self):
        if self.time_end:
            return self.time_end - self.time_executed
        
    def duration(self):
        if self.time_end:
            return self.time_end - self.time_start
        

class TaskRequest(object):
    '''A request for task execution.

 * *controller*: instance of a :class:`Controller`.
 * *task_name*: name of task, must be registered.
 * *task_id*: the id of the task request.
 * *args*: positional arguments for task.
 * *kwargs*: key-value arguments for task.'''
    # Internal flags
    Event         = TaskEvent
    _already_revoked = False
    
    def __init__(self, controller, task_name, task_id, task_modules, args, kwargs, retries=0,
                 expires = None, logger = None):
        self.task         = registry[task_name]
        code              = unique_request_name(self.task, *args, **kwargs)
        self._info        = TaskRequestInfo(task_name, task_id, code, args, kwargs)
        self.task_modules = task_modules
        self.controller   = controller
        self.retries      = retries
        self.expires      = expires
        self.logger       = logger
        self.events       = {'started':self.Event(),
                             'success':self.Event(),
                             'failed':self.Event()}
    
    @property
    def info(self):
        '''Request information, including result, errors, times and so forth.'''
        return self._info
    
    def maybe_expire(self):
        if self.expires and time.time() > self.expires:
            return True
            #state.revoked.add(self.task_id)
            #self.task.backend.mark_as_revoked(self.task_id)
    
    def revoked(self):
        if self._already_revoked:
            return True
        if self.expires:
            self.maybe_expire()
        return False
    
    def execute_using_pool(self, pool):
        '''Execute ``self`` on :class:`unuk.concurrency.ConcurrentPool`.'''
        if self.revoked():
            return
        info = self._info
        info._setexecuted()
        args = (info.name, info.id, self.task_modules) + info.args
        return pool.dispatch(execute_in_process,
                             args = args,
                             kwds = info.kwargs,
                             callback=self.on_success,
                             accept_callback=self.on_accepted,
                             timeout_callback=self.on_timeout,
                             error_callback=self.on_failure)
        
        
    def on_accepted(self):
        info = self._info
        self.fire("started", id=info.id)
        self.logger.debug("Task accepted: %s[%s]" % (info.name, info.id))
        info._setstart()
        
    def on_timeout(self):
        info = self._info
        info._setfinish(True)
        self.fire("failed", id=info.id, timeout=info.timeout)
    
    def on_success(self, result):
        info = self._info
        info._setfinish()
        info.result = result
        self.fire("success", id=info.id, result=result)
        self.logger.debug("Task finished %s[%s]" % (info.name, info.id))
    
    def on_failure(self, exc_info):
        """The handler used if the task raised an exception."""
        info = self._info
        info._setfinish()
        info.exception = repr(exc_info)
        self.fire("failed", id=info.id,
                            timeout = info.timeout,
                            exception = info.exception)
        self.logger.error("Task %s[%s]: %s" % (info.name, info.id, info.exception))

    def fire(self, type, **fields):
        '''Fire a :class:`TaskEvent` instance *type*. If event is not available, nothing happens.
        
 * *type*: event type string ("started","success","failed").
 * *fields*: key-value arguments to fire.'''
        event = self.events.get(type,None)
        if event:
            event.fire(fields)
    
    def bind(self, type, callback):
        '''Bind a *callable* to a :class:`TaskEvent` instance raised by ``self``.
        
 * *type*: the event type, one of "started", "success", "failed"
 * *callback*: callable to call back.'''
        event = self.events.get(type,None)
        if not event:
            raise ValueError("Event %s not available" % type)
        event.bind(callback)
        return self
        