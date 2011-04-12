from multiprocessing.queues import Empty

from .base import WorkerProcess

    
def IOQueue(object):
    
    def __init__(self, queue):
        self._queue = queue
        self._empty = []
        
    def register(self, fd, eventmask = None):
        pass
    
    def modify(self, fd, events):
        pass
    
    def unregister(self, fd):
        pass
    
    def poll(self, timeout):
        try:
            self._queue.get(timeout = timeout)
        except Empty:
            return self._empty            


def get_task_loop(self):
    queue = self.CommandQueue()
    return IOQueue(queue)
    
    
class Worker(WorkerProcess):
    '''A Task worker on a subprocess'''
        
    def get_ioimpl(self):
        return get_task_loop(self)
    
    
        
        