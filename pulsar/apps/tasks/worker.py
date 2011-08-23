from multiprocessing.queues import Empty

import pulsar
from pulsar import system
from .states import PENDING

    
class IOQueue(system.EpollProxy):
    '''The polling mechanism for a task queue. No select or epoll performed here, simply
return task from the queue if available.
This is an interface for using the same IOLoop class of other workers.'''
    def __init__(self, queue):
        super(IOQueue,self).__init__()
        self._queue = queue
        self._fd = id(queue)
        self._empty = []
    
    def fileno(self):
        return self._fd
    
    def poll(self, timeout = 0):
        try:
            req = self._queue.get(timeout = timeout)
            return {self._fd:req}
        except Empty:
            return self._empty


class TaskScheduler(pulsar.WorkerMonitor):
    
    def _addtask(self, caller, task_name, targs, tkwargs, ack = True, **kwargs):
        try:
            task = self.app.make_request(task_name, targs, tkwargs, **kwargs)
            tq = task.to_queue()
            if tq:
                self.task_queue.put((None,tq))
            
            if ack:
                task = tq or task
                return task.tojson_dict()
        except Exception as e:
            return e
    
    def actor_tasks_list(self, caller):
        return self.app.tasks_list()
    
    def actor_addtask(self, caller, task_name, targs, tkwargs, ack=True, **kwargs):
        return self._addtask(caller, task_name, targs, tkwargs, ack = True, **kwargs)
        
    def actor_addtask_noack(self, caller, task_name, targs, tkwargs, ack=False, **kwargs):
        return self._addtask(caller, task_name, targs, tkwargs, ack = False, **kwargs)
    actor_addtask_noack.ack = False
    
    def actor_task_finished(self, caller, response):
        self.app.task_finished(response)
    actor_task_finished.ack = False
    
    def actor_get_task(self, caller, id):
        return self.app.get_task(id)
    
    def actor_job_list(self, caller, jobnames = None):
        return list(self.app.job_list(jobnames = jobnames))
    
    def actor_next_scheduled(self, caller, jobname = None):
        return self.app.scheduler.next_scheduled(jobname = jobname)
        

class Worker(pulsar.Worker):
    '''A Task worker on a subprocess'''
    _class_code = 'TaskQueue'


    

