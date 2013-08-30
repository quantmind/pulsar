from multiprocessing import dummy, current_process
from threading import Lock
from functools import partial

try:
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue
Empty = queue.Empty
Full = queue.Full

from pulsar.utils.pep import set_event_loop, new_event_loop
from pulsar.utils.exceptions import StopEventLoop

from .access import get_actor, set_actor, thread_local_data, LOGGER
from .defer import Deferred, safe_async
from .pollers import Poller, READ


__all__ = ['Thread', 'IOqueue', 'ThreadPool', 'ThreadQueue', 'Empty', 'Full']

class Thread(dummy.DummyProcess):
    
    @property
    def pid(self):
        return current_process().pid
    
    def loop(self):
        raise NotImplemented

    def terminate(self):
        '''Invoke the stop on the event loop method.'''
        if self.is_alive():
            loop = self.loop()
            if loop:
                loop.stop()
            else:
                LOGGER.error('Cannot terminate thread. No loop.')            
    
    
class PoolThread(Thread):
    '''This class should be used when creating CPU threads in pulsar. It
makes sure the class:`Actor` controlling the thread is available.'''
    def __init__(self, actor, *args, **kwargs):
        self._actor = actor
        super(PoolThread, self).__init__(*args, **kwargs)
        self.name = '%s-%s' % (self._actor, self.name)
    
    def __repr__(self):
        if self.ident:
            return '%s-%s' % (self.name, self.ident)
        else:
            return self.name
    __str__ = __repr__
    
    def run(self):
        '''Modified run method which set the actor and the event_loop for
the running thread.'''
        actor = self._actor
        del self._actor
        set_actor(actor)
        set_event_loop(actor.event_loop)
        super(Thread, self).run()
        
    def loop(self):
        return thread_local_data('_request_loop', ct=self)


class IOqueue(Poller):

    def __init__(self, queue, maxtasks):
        super(IOqueue, self).__init__()
        self._queue = queue
        self._maxtasks = maxtasks
        self._wakeup = 0
        self.received = 0
        self.completed = 0
        self.lock = Lock()
        
    @property
    def cpubound(self):
        '''Required by the :class:`EventLoop` so that the event loop
install itself as a request loop rather than the IO event loop.'''
        return True
    
    def poll(self, timeout=0.5):
        if self._maxtasks and self.received >= self._maxtasks:
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
        return ((self.fileno(), task),)
    
    def install_waker(self, event_loop):
        return self
    
    def wake(self):
        '''Waker implementation. This IOqueue is its own waker.'''
        with self.lock:
            self._wakeup += 1
            
    def check_stream(self):
        raise IOError('Cannot use stream interface')
    
    def handle_events(self, loop, fd, task):
        try:
            self._handlers[fd]
        except KeyError:
            raise KeyError('Received an event on unregistered file '
                           'descriptor %s' % fd)
        self.received += 1
        future, func, args, kwargs = task
        result = safe_async(func, *args, **kwargs)
        result.add_both(partial(self._handle_result, future))
        return future
                
    def get(self, timeout=0.5):
        '''Wait for events. timeout in seconds (float)'''
        block = True
        with self.lock:
            if self._wakeup:
                block = False
                self._wakeup -= 1
        return self._queue.get(block=block, timeout=timeout)
    
    def _register(self, fd, events, old_events=None):
        if events != READ:
            raise IOError('Only read events can be attached to IOqueue')
        if fd != self.fileno():
            raise IOError('Only read events on %s allowed' % self.fileno())
        
    def _handle_result(self, future, result):
        self.completed += 1
        future.callback(result)
        

RUN = 0
CLOSE = 1
TERMINATE = 2

class ThreadPool(object):
    '''A thread pool for an actor.
    
This pool maintains a group of threads to perform asynchronous tasks via
the :meth:`apply` method.'''
    def __init__(self, actor=None, threads=None, check_every=5, maxtasks=None):
        self._actor = actor or get_actor()
        self._check_every = check_every
        self._threads = max(threads or 1, 1)
        self._pool = []
        self._state = RUN
        self._closed = Deferred(event_loop=self.event_loop)
        self._maxtasks = maxtasks
        self._inqueue = ThreadQueue()
        self._check = self.event_loop.call_soon(self._maintain)
    
    @property
    def status(self):
        '''String status of this pool.'''
        if self._state == RUN:
            return 'running'
        elif self._state == CLOSE:
            return 'closed'
        elif self._state == TERMINATE:
            return 'terminated'
        else:
            return 'unknown'
        
    @property
    def event_loop(self):
        '''The event loop running this :class:`ThreadPool`.'''
        return self._actor.event_loop
    
    @property
    def num_threads(self):
        '''Number of threads in the pool.'''
        return len(self._pool)
    
    def apply(self, func, *args, **kwargs):
        '''Equivalent to ``func(*args, **kwargs)``.
    
This method create a new task for function ``func`` and adds it to the queue.
Return a :class:`Deferred` called back once the task has finished.'''
        assert self._state == RUN, 'Pool not running'
        d = Deferred()
        self._inqueue.put((d, func, args, kwargs))
        
    def close(self, timeout=None):
        '''Close the thread pool.
        
Return a :class:`Deferred` fired when all threads have exited.'''
        if self._state == RUN:
            self._state = CLOSE
            if self.event_loop.is_running():
                self.event_loop.call_now_threadsafe(self._close)
            else:
                self._close()
        return self._closed.then().set_timeout(timeout)
            
    def terminate(self, timeout=None):
        '''Shut down the event loop of threads'''
        if self._state < TERMINATE:
            if not self._closed.done():
                self._state = TERMINATE
                if self.event_loop.is_running():
                    self.event_loop.call_now_threadsafe(self._terminate)
                else:
                    self._terminate()
        return self._closed.then().set_timeout(timeout)
            
    def join(self):
        assert self._state in (CLOSE, TERMINATE)
        for worker in self._pool:
            worker.join()
        
    def _maintain(self):
        populate = self._join_exited_workers() if self._pool else True
        if self._state == RUN and populate:
            self._repopulate_pool()
        elif self._state == CLOSE:
            self._close_pool()
        if self._pool:
            self._maintain_call = self._actor.event_loop.call_later(
                self._check_every, self._maintain)
        elif not self._closed.done():
            self._closed.callback(self._state)
    
    def _close(self):
        self._check.cancel()
        self._check = None
        self._check_every = 0
        self._maintain()
    
    def _terminate(self):
        for worker in self._pool:
            worker.terminate()
        self._maintain()
            
    def _join_exited_workers(self):
        """Cleanup after any worker processes which have exited due to reaching
        their specified lifetime.  Returns True if any workers were cleaned up.
        """
        cleaned = []
        for worker in self._pool:
            if worker.exitcode is not None:
                # worker exited
                self._actor.logger.debug('Cleaning up %s', worker)
                worker.join()
                cleaned.append(worker)
        if cleaned:
            for c in cleaned:
                self._pool.remove(c)
        return bool(cleaned)
        
    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        while len(self._pool) < self._threads:
            worker = PoolThread(self._actor, name='PoolWorker',
                                target=self._run)
            self._pool.append(worker)
            worker.daemon = True
            worker.start()
            self._actor.logger.debug('Added %s', worker)
            
    def _close_pool(self):
        for i in range(len(self._pool)):
            self._inqueue.put(None)
            
    def _run(self):
        poller = IOqueue(self._inqueue, self._maxtasks)
        # Create the event loop which get tasks from the task queue 
        event_loop = new_event_loop(io=poller, poll_timeout=1,
                                    logger=self._actor.logger)
        event_loop.add_reader(poller.fileno(), poller.handle_events)
        event_loop.run_forever()
        