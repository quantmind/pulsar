from multiprocessing.queues import Empty
from pulsar import system
from .base import WorkerProcess, updaterequests

    
class IOQueue(system.EpollProxy):
    '''The polling mechanism for a task queue. No select or epoll performed, simply
get tasks from the districuted task queue.
This is an interface for using the same IOLoop class as other workers.'''
    def __init__(self, queue = None):
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


def get_task_loop(self):
    queue = self.task_queue
    return IOQueue(queue)


def start_task_loop(self):
    ioloop = self.ioloop
    impl = ioloop._impl
    ioloop._handlers[impl.fileno()] = self.handle_request
    self.ioloop.start()
    

class TaskMixin(object):
    
    @updaterequests
    def handle_task(self, fd, req):
        self.handler(req)
    
    
class Worker(TaskMixin, WorkerProcess):
    '''A Task worker on a subprocess'''
        
    def get_ioimpl(self):
        return get_task_loop(self)
    
    def _run(self):
        start_task_loop(self)
    
