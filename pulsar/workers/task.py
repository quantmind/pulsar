from multiprocessing.queues import Empty

import pulsar
from pulsar import system

    
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
    
    def actor_addtask(self, task_name, targs, tkwargs, **kwargs):
        request = self.app.make_request(task_name, targs, tkwargs, ack=True, **kwargs)
        self.task_queue.put(request)
        return request
        
    def actor_addtask_noack(self, task_name, targs, tkwargs, **kwargs):
        request = self.app.make_request(task_name, targs, tkwargs, **kwargs)
        self.task_queue.put(request)
    actor_addtask_noack.ack = False
        

class Worker(pulsar.Worker):
    '''A Task worker on a subprocess'''
    _class_code = 'TaskQueue'


    

