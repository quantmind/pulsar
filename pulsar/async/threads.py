import sys
from multiprocessing import pool, dummy
from threading import Lock
from functools import partial

try:
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue
Empty = queue.Empty

from pulsar.utils.system import EpollInterface
from pulsar.utils.log import LocalMixin, local_property
from pulsar.utils.pep import set_event_loop

from .access import get_actor, set_actor
from .eventloop import StopEventLoop, EventLoop
from .defer import maybe_async, is_async, log_failure


__all__ = ['Thread', 'IOQueue', 'ThreadPool', 'ThreadQueue', 'Empty']


class Thread(dummy.DummyProcess):
    '''This class should be used when creating threads in pulsar. It
makes sure the class:`Actor` controlling the thread is available.'''
    def __init__(self, *args, **kwargs):
        self.actor = get_actor()
        super(Thread, self).__init__(*args, **kwargs)
        
    def run(self):
        '''Modified run method which set the actor and the event_loop for
the running thread.'''
        actor = self.actor
        del self.actor
        set_actor(actor)
        set_event_loop(actor.event_loop)
        super(Thread, self).run()


class _FdFactory:
    
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

.. attribute:: queue

    The python ``Queue`` from where this poller get tasks at each
    iteration of the :class:`EventLoop`
    
.. attribute:: maxtasks

    Optional number of maximum tasks to process
    
.. attribute:: received

    Number of tasks received by this :class:`IOQueue`
    
.. attribute:: completed

    Number of tasks completed by this :class:`IOQueue`

.. _epoll: http://docs.python.org/library/select.html#epoll-objects'''
    fd_factory = _FdFactory
    
    def __init__(self, queue, maxtasks=None):
        assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)
        self.received = 0
        self.completed = 0
        self._wakeup = 0
        self._queue = queue
        self.maxtasks = maxtasks

    @property
    def cpubound(self):
        '''Required by the :class:`EventLoop` so that the event loop
install itself as a request loop rather than the IO event loop.'''
        return True
    
    def fileno(self):
        '''dummy file number'''
        return 0
    
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
            if self.completed < self.received:
                return ()
            else:
                raise StopEventLoop
        try:
            task = self.get(timeout=timeout)
        except (Empty, TypeError):
            return ()
        except (EOFError, IOError):
            raise StopEventLoop
        if task is None:    # got the sentinel, exit!
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
    '''A pool worker which handles asynchronous results form
functions send to the task queue. Instances of this class are
initialised on a new pulsar :class:`Thread`.'''
    def __init__(self, inqueue, outqueue, initializer=None, initargs=(),
                 maxtasks=None):
        if hasattr(inqueue, '_writer'):
            inqueue._writer.close()
            outqueue._reader.close()
        if initializer is not None:
            initializer(*initargs)
        self.io_poller = IOQueue(inqueue, maxtasks)
        self.outqueue = outqueue
        # Create the event loop which get tasks from the task queue 
        event_loop = EventLoop(io=self.io_poller, poll_timeout=1)
        event_loop.add_reader(self.io_poller.fileno(), self._handle_request)
        event_loop.run_forever()
        
    def _handle_request(self, task):
        job, i, func, args, kwds = task
        try:
            result = maybe_async(func(*args, **kwds))
        except Exception:
            result = maybe_failure(sys.exc_info())
        if is_async(result):
            result.add_both(partial(self._handle_result, job, i))
        else:
            self._handle_result(job, i, result)
            
    def _handle_result(self, job, i, result):
        log_failure(result)
        self.outqueue.put((job, i, (True, result)))
        self.io_poller.completed += 1


class ThreadPool(pool.ThreadPool):
    '''A modified :class:`multiprocessing.pool.ThreadPool` used by a pulsar
:class:`Actor` when it needs CPUbound workers to consume tasks
on a task queue. An actor can create a new :class:`ThreadPool` via
the :meth:`Actor.create_thread_pool` method.'''
    Process = Thread
    
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
    
    def apply(self, *args, **kwargs):
        raise NotImplementedError('Use apply_async')