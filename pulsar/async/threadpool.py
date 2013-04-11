import sys
from multiprocessing import pool
from threading import Lock
from functools import partial

try:    #pragma nocover
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue
Empty = queue.Empty

from pulsar.utils.system import EpollInterface
from pulsar.utils.log import LocalMixin, local_property

from .eventloop import StopEventLoop, EventLoop


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
            raise RuntimeError("Already reading!")
        
    def add_writer(self, callback):
        pass
        
    def remove_connector(self):
        pass
    
    def remove_writer(self):
        pass
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        self.handle_read = None
    
    def __call__(self, request):
        return self.handle_read(request)


class IOQueue(EpollInterface, LocalMixin):
    '''Epoll like class for a IO based on queues rather than sockets.
The interface is the same as the python epoll_ implementation.

.. _epoll: http://docs.python.org/library/select.html#epoll-objects'''
    fd_factory = TaskFactory
    
    def __init__(self, queue, maxtasks=None):
        assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)
        self._wakeup = 0
        self.received = 0
        self.completed = 0
        self._queue = queue
        self.maxtasks = maxtasks

    def fileno(self):
        '''dummy file number'''
        return 1
    
    def register(self, fd, events=None):
        pass

    def modify(self, fd, events=None):
        pass

    def unregister(self, fd):
        pass

    def get(self, timeout=0.5):
        '''Wait for events. timeout in seconds (float)'''
        block = True
        with self.lock:
            if self._wakeup:
                block = False
                self._wakeup -= 1
        return self._queue.get(block=block, timeout=timeout)
    
    def poll(self, timeout=0.5):
        if self.maxtasks and self.received >= self.maxtasks:
            if self.received == self.completed:
                raise StopEventLoop
            else:
                return ()
        try:
            task = self.get(timeout=timeout)
        except (Empty, TypeError):
            return ()
        except (EOFError, IOError):
            raise StopEventLoop
        if task is None:
            raise StopEventLoop
        self.received += 1
        return ((self.fileno(), task),)
    
    def install_waker(self, loop):
        return self
        
    def wake(self):
        '''Waker implementation. This IOQueue is its own waker.'''
        with self.lock:
            self._wakeup += 1
            
    @local_property
    def lock(self):
        return Lock()


class PoolWorker(object):
    
    def __init__(self, inqueue, outqueue, initializer=None, initargs=(),
                 maxtasks=None):
        if hasattr(inqueue, '_writer'):
            inqueue._writer.close()
            outqueue._reader.close()
        if initializer is not None:
            initializer(*initargs)
        io_poller = IOQueue(inqueue, maxtasks)
        self.outqueue = outqueue
        self.event_loop = EventLoop(io=io_poller)
        self.event_loop.add_reader(io_poller.fileno(), self._handle_request)
        self.event_loop.run_forever()
        
    def _handle_request(self, task):
        job, i, func, args, kwds = task
        result = maybe_async()
        try:
            result = maybe_async(func(*args, **kwds),
                                 event_loop=self.event_loop)
        except Exception:
            result = maybe_failure(sys.exc_info())
        if is_async(result):
            result.add_both(partial(self._handle_result, job, i))
        else:
            self._handle_result(job, i, result)
            
    def _handle_result(self, job, i, result):
        success = not is_failure(result)
        self.outqueue.put((job, i, (success, result)))
        self.event_loop.io.completed += 1


class ThreadPool(pool.ThreadPool):
    
    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            w = self.Process(target=PoolWorker,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs, self._maxtasksperchild)
                            )
            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            w.start()
            