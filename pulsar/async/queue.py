from multiprocessing.queues import Empty, Queue

from pulsar.utils.system import IObase

__all__ = ['Empty', 'Queue', 'IOQueue']

class QUEUE_WAKER:
    pass


class QueueWaker(object):
    '''A waker for :class:`IOQueue`. Used by CPU-bound actors.'''
    def __init__(self, queue):
        self._queue = queue
        self._fd = 'waker'

    def __str__(self):
        return '%s %s' % (self.__class__.__name__, self._fd)

    def fileno(self):
        return self._fd

    def wake(self):
        try:
            self._queue.put(QUEUE_WAKER)
        except (IOError,TypeError):
            pass

    def consume(self):
        pass

    def close(self):
        pass


class TaskFactory:
    
    def __init__(self, fd, eventloop, read=None, **kwargs):
        self.fd = fd
        self.eventloop = eventloop
        self.handle_read = None
        self.add_reader(read)
    
    def add_connector(self, callback):
        pass
        
    def add_reader(self, callback):
        if not self.handle_read:
            self.handle_read = callback
        else:
            raise RuntimeError("Aalready reading!")
        
    def add_writer(self, callback):
        pass
        
    def remove_connector(self):
        pass
    
    def remove_writer(self):
        pass
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        self.handle_read = None
    
    def __call__(self, fd, request):
        self.handle_read(request)


class IOQueue(IObase):
    '''Epoll like class for a IO based on queues rather than sockets.
The interface is the same as the python epoll_ implementation.

.. _epoll: http://docs.python.org/library/select.html#epoll-objects'''
    cpubound = True
    fd_factory = TaskFactory
    def __init__(self, queue, actor=None):
        self._queue = queue
        self._actor = actor
        self.request = None

    @property
    def queue(self):
        '''The underlying distributed queue used for I/O.'''
        return self._queue

    def register(self, fd, events=None):
        pass

    def modify(self, fd, events=None):
        pass

    def unregister(self, fd):
        pass

    def poll(self, timeout=0.5):
        '''Wait for events. timeout in seconds (float)'''
        if self._actor:
            if not self._actor.can_poll():
                return ()
        try:
            request = self._queue.get(timeout=timeout)
        except (Empty,IOError, TypeError, EOFError):
            return ()
        if request == QUEUE_WAKER:
            return ()
        else:
            return (('request', request),)

    def waker(self):
        return QueueWaker(self._queue)