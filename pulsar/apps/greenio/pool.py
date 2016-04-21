import sys
import threading
import logging
from collections import deque

from pulsar import Future, ensure_future, AsyncObject, get_event_loop

from .utils import wait, GreenletWorker, isawaitable, getcurrent


_DEFAULT_WORKERS = 100
_MAX_WORKERS = 1000


class _DONE:
    pass


class GreenPool(AsyncObject):
    """A pool of running greenlets.

    This pool maintains a group of greenlets to perform asynchronous
    tasks via the :meth:`submit` method.
    """
    worker_name = 'exec'

    def __init__(self, max_workers=None, loop=None):
        self._loop = loop or get_event_loop()
        self._max_workers = min(max_workers or _DEFAULT_WORKERS, _MAX_WORKERS)
        self._greenlets = set()
        self._available = set()
        self._queue = deque()
        self._shutdown = False
        self._waiter = None
        self._logger = logging.getLogger('pulsar.greenpool')
        self._shutdown_lock = threading.Lock()
        self.wait = wait

    @property
    def max_workers(self):
        return self._max_workers

    @max_workers.setter
    def max_workers(self, value):
        value = int(value)
        assert value > 0
        self._max_workers = value

    @property
    def in_green_worker(self):
        """True if the current greenlet is a green pool worker
        """
        return isinstance(getcurrent(), GreenletWorker)

    def submit(self, func, *args, **kwargs):
        """Equivalent to ``func(*args, **kwargs)``.

        This method create a new task for function ``func`` and adds it to
        the queue.
        Return a :class:`~asyncio.Future` called back once the task
        has finished.
        """
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError(
                    'cannot schedule new futures after shutdown')
            if self.in_green_worker:
                return wait(func(*args, **kwargs))
            else:
                future = Future(loop=self._loop)
                self._put((future, func, args, kwargs))
                return future

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._put(None)
            if wait:
                self._waiter = Future(loop=self._loop)
                return self._waiter

    def getcurrent(self):
        return getcurrent()

    # INTERNALS
    def _adjust_greenlet_count(self):
        if (not self._shutdown and not self._available and
                len(self._greenlets) < self._max_workers):
            green = GreenletWorker(self._green_run)
            self._greenlets.add(green)
            self.logger.debug('Num greenlets: %d', len(self._greenlets))
            green.switch()
        return self._available

    def _put(self, task):
        # Run in the main greenlet of the evnet-loop thread
        self._queue.appendleft(task)
        self._check_queue()

    def _check_queue(self):
        # Run in the main greenlet of the event-loop thread
        if not self._adjust_greenlet_count():
            self.logger.debug('No greenlet available')
            return self._loop.call_soon(self._check_queue)
        try:
            task = self._queue.pop()
        except IndexError:
            return
        ensure_future(self._green_task(self._available.pop(), task),
                      loop=self._loop)

    async def _green_task(self, green, task):
        # Coroutine executing the in main greenlet
        # This coroutine is executed for every task put into the queue

        while task is not _DONE:
            # switch to the greenlet to start the task
            task = green.switch(task)

            # if an asynchronous result is returned, await
            while isawaitable(task):
                try:
                    task = await task
                except Exception as exc:
                    # This call can return an asynchronous component
                    exc_info = sys.exc_info()
                    if not exc_info[0]:
                        exc_info = (exc, None, None)
                    task = green.throw(*exc_info)

    def _green_run(self):
        # The run method of a worker greenlet
        task = True
        while task:
            green = getcurrent()
            parent = green.parent
            assert parent
            # add greenlet in the available greenlets
            self._available.add(green)
            task = parent.switch(_DONE)  # switch back to the main execution
            if task:
                future, func, args, kwargs = task
                try:
                    try:
                        result = wait(func(*args, **kwargs), True)
                    except StopIteration as exc:  # See PEP 479
                        raise RuntimeError('Unhandled StopIteration') from exc
                except Exception as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            else:  # Greenlet cleanup
                self._greenlets.remove(green)
                if self._greenlets:
                    self._put(None)
                elif self._waiter:
                    self._waiter.set_result(None)
                    self._waiter = None
                parent.switch(_DONE)
