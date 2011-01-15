import os
import threading
import logging

from unuk.utils import gen_unique_id
from unuk.utils.importer import import_modules
from unuk.contrib.tasks.registry import registry
from unuk.contrib.tasks.request import TaskEvent, TaskRequest, requestinfo, unique_request_name
from unuk.contrib.tasks.scheduler import Scheduler, SchedulerEntry


class ThreadedClockService(threading.Thread):
    
    def __init__(self, service):
        threading.Thread.__init__(self)
        self.service = service

    def run(self):
        self.service.run()
    
    def stop(self):
        self.service.stop()
        
    def wait(self):
        self.service.stop(wait = True)
        

def get_schedule():
    schedule = {}
    for name,task in registry.periodic().items():
        schedule[name] = {'name': name,
                          'schedule': task.run_every,
                          'anchor': task.anchor}
    return schedule


class Controller(object):
    '''The task's controller maintains a process pool to where tasks are queued.
This class should be used as a singletone__ on the server application.

    .. attribute:: pool
    
        instance of :class:unuk.concurrency.ConcurrentPool
        
    .. attribute:: task_modules
    
        list of python dotted path to task modules
        
    .. attribute:: scheduler
    
        a periodic task schedule
    

__ http://en.wikipedia.org/wiki/Singleton_pattern
'''
    Request = TaskRequest
    
    PeriodicScheduler = Scheduler
    ''' Scheduler class for handling :class:`unuk.contrib.tasks.PeriodicTask`.'''
    
    def __init__(self, pool = None, task_modules = None, beat = 5):
        self.pool         = pool
        self.task_modules = task_modules
        import_modules(task_modules)
        self._cache       = {}
        self._cachecode   = {}
        self.logger = logging.getLogger('unuk.task.Controller')
        schedule  = get_schedule()
        scheduler = self.PeriodicScheduler(entries = schedule,
                                           controller = self,
                                           beat = beat)
        self.scheduler  = scheduler
        self._scheduler = ThreadedClockService(scheduler)
    
    def info(self):
        return self.pool.info()
    
    def liverequests(self):
        '''Return a list of serializable live requests'''
        info = []
        for request in self._cache.values():
            if request.info:
                info.append(request.info.todict())
        return info
    
    def start(self):
        '''Starts the controller. Return ``self``.
Typical constructor and start statement::

    controller = Controller(processes = 2).start()'''
        if self.pool and not self.pool.started:
            self.pool.start()
        self._scheduler.start()
        return self
        
    def stop(self):
        if self._scheduler:
            self._scheduler.wait()
        if self.pool:
            self.pool.stop()
        if self._scheduler:
            self._scheduler.wait()
        
    def wait(self, sleep = None):
        if self._scheduler:
            self._scheduler.stop()
        if self.pool:
            self.pool.wait(sleep = sleep)
        
    def dispatch(self, task_name, *args, **kwargs):
        '''Dispatch a task to the processing pool.

Returns an instance of :class:`TaskRequest`.'''
        task_id = gen_unique_id()
        self.logger.debug('Dispatching task %s to the processing pool' % task_id)
        request = self.Request(self,
                               task_name,
                               task_id,
                               self.task_modules,
                               args,
                               kwargs,
                               logger = self.logger)
        code = request.info.code
        old_request = self._cachecode.get(code,None)
        if old_request:
            if old_request.info.time_end:
                self._cachecode.pop(code)
            return old_request
        finished = self.request_finished
        request.bind('success',finished).bind('failed',finished)
        self._add(request)
        request.execute_using_pool(self.pool)
        return request
    
    def _add(self, request):
        info = request.info
        self._cache[info.id]       = request
        self._cachecode[info.code] = request
                
    def request_finished(self, id = None, **kwargs):
        if id:
            self._cache.pop(id,None)
        
    def get_task(self, id):
        '''Fetch a working :class:`TaskRequest` with identy *id* if available.'''
        return self._cache.get(id, None)
    