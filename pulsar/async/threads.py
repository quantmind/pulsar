import logging
import threading
import weakref
from functools import partial
from multiprocessing import dummy, current_process

try:
    import queue
except ImportError:  # pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue
Empty = queue.Empty
Full = queue.Full

from .access import (asyncio, selectors, get_actor, set_actor,
                     events, thread_data, _StopError, BaseEventLoop,
                     get_event_loop, logger as get_logger)
from .futures import Future, Task, async, AsyncObject
from .consts import ACTOR_STATES


__all__ = ['Thread', 'IOqueue', 'ThreadPool',
           'ThreadQueue', 'Empty', 'Full']

_MAX_WORKERS = 50
_threads_queues = weakref.WeakKeyDictionary()


def get_executor(loop):
    executor = loop._default_executor
    if executor is None:
        executor = ThreadPool(loop=loop)
        loop._default_executor = executor
    return executor


def run_in_executor(loop, executor, callback, *args):
    if isinstance(callback, events.Handle):
        assert not args
        assert not isinstance(callback, events.TimerHandle)
        if callback._cancelled:
            f = Future(loop=loop)
            f.set_result(None)
            return f
        callback, args = callback._callback, callback._args
    if executor is None:
        executor = get_executor(loop)
    return executor.submit(callback, *args)


class Thread(dummy.DummyProcess):
    _loop = None
    _pool_loop = None

    @property
    def pid(self):
        return current_process().pid

    def terminate(self):
        '''Invoke the stop on the event loop method.'''
        if self.is_alive() and self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def set_loop(self, loop):
        assert self._loop is None
        self._loop = loop


class PoolThread(Thread):
    '''A thread for the :class;`.ThreadPool`.
    '''
    def __init__(self, pool):
        self.pool = pool
        self._pool_loop = pool._loop
        super(PoolThread, self).__init__(name=pool.worker_name)

    def __repr__(self):
        if self.ident:
            return '%s-%s' % (self.name, self.ident)
        else:
            return self.name
    __str__ = __repr__

    def run(self):
        '''Modified run method which set the actor and the event_loop.
        '''
        if self.pool._actor:
            set_actor(self.pool._actor)
        # The run method for the threads in this thread pool
        logger = logging.getLogger('pulsar.%s' % self.name)
        loop = QueueEventLoop(self.pool, logger=logger, iothreadloop=True)
        self.set_loop(loop)
        loop.run_forever()


class IOqueue(selectors.BaseSelector):
    '''A selector based on a distributed queue

    Since there is no way to my knowledge to wake up the queue while
    getting an item from the task queue, the timeout cannot be larger than
    a small number which by default is ``0.5`` seconds.
    '''
    max_timeout = 0.5

    def __init__(self, executor):
        super(IOqueue, self).__init__()
        self._actor = executor._actor
        self._work_queue = executor._work_queue
        self._maxtasks = executor._maxtasks
        self._received = 0
        self._completed = 0

    def select(self, timeout=None):
        if self._actor and self._actor.state > ACTOR_STATES.RUN:
            raise _StopError
        if self._maxtasks and self._received >= self._maxtasks:
            if self._completed < self._received:
                return ()
            else:
                raise _StopError
        block = True
        if timeout is None:
            timeout = self.max_timeout
        elif timeout <= 0:
            timeout = 0
            block = False
        else:
            timeout = min(self.max_timeout, timeout)
        try:
            task = self._work_queue.get(block=block, timeout=timeout)
        except (Empty, TypeError):
            return ()
        except (EOFError, IOError):
            raise _StopError
        if task is None:    # got the sentinel, exit!
            self._work_queue.put(None)
            raise _StopError
        return task

    def process_task(self, task, loop):
        self._received += 1
        p, func, args, kwargs = task
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            self._done_task(p, None, exc)
        else:
            try:
                result = async(result, loop=loop)
            except TypeError:
                self._done_task(p, None, result)
            else:
                result.add_done_callback(partial(self._done_task, p))

    def _done_task(self, p, future, result=None):
        self._completed += 1
        #
        if future:
            try:
                result = future.result()
            except Exception as exc:
                result = exc
        #
        if isinstance(result, Exception):
            p._loop.call_soon_threadsafe(lambda: p.set_exception(result))
        else:
            p._loop.call_soon_threadsafe(lambda: p.set_result(result))

    def get_map(self):
        return {}

    def register(self, fileobj, events, data=None):
        pass

    def unregister(self, fileobj):
        pass


class ThreadSafeLoop(object):

    def __init__(self, iothreadloop):
        self._iothreadloop = iothreadloop
        if self._iothreadloop:
            self._original_call_soon = self.call_soon
            self.call_soon = self._threadsafe_call_soon
            asyncio.set_event_loop(self)

    def _threadsafe_call_soon(self, callback, *args):
        if self != get_event_loop():
            return self.call_soon_threadsafe(callback, *args)
        else:
            return self._original_call_soon(callback, *args)


class QueueEventLoop(BaseEventLoop, ThreadSafeLoop):
    '''An :ref:`asyncio event loop <asyncio-event-loop>` which
    uses :class:`.IOqueue` as its selector.
    '''
    def __init__(self, executor, iothreadloop=False, logger=None):
        super(QueueEventLoop, self).__init__()
        ThreadSafeLoop.__init__(self, iothreadloop)
        self._default_executor = executor
        self._selector = IOqueue(executor)
        self.logger = get_logger(logger=logger)

    def create_task(self, coro):
        return Task(coro, loop=self)

    def _write_to_self(self):
        pass

    def _process_events(self, task):
        if task:
            self._selector.process_task(task, self)

    def run_in_executor(self, executor, callback, *args):
        return run_in_executor(self, executor, callback, *args)


class ThreadPool(AsyncObject):
    '''A thread pool for an actor.

    This pool maintains a group of threads to perform asynchronous tasks via
    the :meth:`submit` method.
    '''
    worker_name = 'exec'

    def __init__(self, max_workers=None, actor=None, loop=None,
                 maxtasks=None):
        self._actor = actor = actor or get_actor()
        if actor:
            loop = loop or actor._loop
            if not max_workers:
                max_workers = actor.cfg.thread_workers
            self.worker_name = '%s.%s' % (actor.name, self.worker_name)
        self._loop = loop or get_event_loop()
        self._max_workers = min(max_workers or _MAX_WORKERS, _MAX_WORKERS)
        self._threads = set()
        self._maxtasks = maxtasks
        self._work_queue = ThreadQueue()
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    def submit(self, func, *args, **kwargs):
        '''Equivalent to ``func(*args, **kwargs)``.

        This method create a new task for function ``func`` and adds it to
        the queue.
        Return a :class:`~asyncio.Future` called back once the task
        has finished.
        '''
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    'cannot schedule new futures after shutdown')
            future = Future(loop=self._loop)
            self._work_queue.put((future, func, args, kwargs))
            self._adjust_thread_count()
            return future

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(None)
        if wait:
            for t in self._threads:
                t.join()

    def _adjust_thread_count(self):
        if len(self._threads) < self._max_workers:
            t = PoolThread(self)
            t.daemon = True
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue
