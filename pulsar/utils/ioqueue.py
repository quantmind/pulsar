from .system import IOselect
from .tools import gen_unique_id


class QueueWaker(object):
    
    def __init__(self, queue):
        self._queue = queue
        self._fd = gen_unique_id()[:8]
        
    def fileno(self):
        return self._fd
    
    def wake(self):
        try:
            self._queue.put((self._fd,None))
        except IOError:
            pass
        
    def consume(self):
        pass
    
    def close(self):
        pass
        
        
class IOQueue(object):
    '''Epoll like class for a IO based on queues.
No select or epoll performed here, data is retrieved from a queue.'''
    def __init__(self, queue):
        self._queue = queue
        self._fds = set()
        self._empty = ()

    def register(self, fd, events = None):
        self._fds.add(fd)
                
    def modify(self, fd, events = None):
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        self._fds.discard(fd)
    
    def poll(self, timeout = 0):
        try:
            event = self._queue.get(timeout)
        except Empty:
            return self._empty
        except IOError:
            return self._empty
        
        return (event,)
    
    def waker(self):
        return QueueWaker(self._queue)
    
    
        