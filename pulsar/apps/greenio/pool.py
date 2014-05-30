import threading
from collections import deque

import greenlet
from greenlet import getcurrent

from pulsar import Future, get_event_loop, AsyncObject, task


_DEFAULT_WORKERS = 100
_MAX_WORKERS = 1000


class GreenletWorker(greenlet.greenlet):
    pass


class GreenPool(AsyncObject):
    '''A pool of running greenlets.

    This pool maintains a group of greenlets to perform asynchronous tasks via
    the :meth:`submit` method.
    '''
    worker_name = 'exec'

    def __init__(self, max_workers=None, loop=None, maxtasks=None):
        self._loop = loop or get_event_loop()
        self._max_workers = min(max_workers or _DEFAULT_WORKERS, _MAX_WORKERS)
        self._greenlets = set()
        self._available = set()
        self._maxtasks = maxtasks
        self._queue = deque()
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
            self._loop.call_soon(self._put, (future, func, args, kwargs))
            return future

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._loop.call_soon(self._put)

    # INTERNALS
    def _adjust_greenlet_count(self):
        if len(self._greenlets) < self._max_workers:
            greenlet = GreenletWorker(self._green_run)
            self._greenlets.add(greenlet)
            greenlet.switch()

    def _check_queue(self):
        # Run in the main greenlet of the evnet-loop thread
        try:
            task = self._queue.pop()
        except IndexError:
            return
        if self._available:
            self._green_task(task)

    @task
    def _green_task(self, task):
        # Run in the main greenlet of the evnet-loop thread
        greenlet = self._available.pop()
        result = greenlet.switch(task)
        while isinstance(result, Future):
            result = greenlet.switch((yield result))

    def _put(self, task=None):
        # Run in the main greenlet of the evnet-loop thread
        if task:
            self._adjust_greenlet_count()
        self._queue.appendleft(task)
        self._check_queue()

    def _green_run(self):
        # The run method of a worker greenlet
        current = getcurrent()
        parent = current.parent
        assert parent
        task = True
        while task:
            self._available.add(current)
            self._loop.call_soon(self._check_queue)
            task = parent.switch()  # switch back to the main execution
            if task:
                future, func, args, kwargs = task
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            else:
                self._greenlets.pop(current)
                self._put(None)


class RunInPool:
    '''Utility for running a callable in a :class:`.GreenPool`.

    :param app: the callable to run on greenlet workers
    :param max_workers=100: maximum number of workers
    :param loop: optional event loop

    THis utility is used by the :mod:`~pulsar.apps.pulse` application.
    '''
    def __init__(self, app, max_workers=None, loop=None):
        self.pool = GreenPool(max_workers=max_workers, loop=loop)
        self.app = app

    def __call__(self, *args, **kwargs):
        return self.pool.submit(self.app, *args, **kwargs)
