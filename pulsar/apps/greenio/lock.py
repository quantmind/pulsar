import threading
from collections import deque

from greenlet import getcurrent

from pulsar import get_event_loop, Future

from .utils import MustBeInChildGreenlet


class GreenLock:
    """A Locking primitive that is owned by a particular greenlet
    when locked.The main greenlet cannot acquire the lock.

    A primitive lock is in one of two states, 'locked' or 'unlocked'.

    It is created in the unlocked state. It has two basic methods,
    :meth:`.acquire` and :meth:`.release. When the state is unlocked,
    :meth:`.acquire` changes the state to locked and returns immediately.

    When the state is locked, :meth:`.acquire` blocks the current greenlet
    until a call to :meth:`.release` changes it to unlocked,
    then the :meth:`.acquire` call resets it to locked and returns.
    """
    def __init__(self, loop=None):
        self._loop = loop or get_event_loop()
        self._local = threading.local()
        self._local.locked = None
        self._queue = deque()

    def locked(self):
        """'Return the greenlet that acquire the lock or None.
        """
        return self._local.locked

    def acquire(self, timeout=None):
        """Acquires the lock if in the unlocked state otherwise switch
        back to the parent coroutine.
        """
        green = getcurrent()
        parent = green.parent
        if parent is None:
            raise MustBeInChildGreenlet('GreenLock.acquire in main greenlet')

        if self._local.locked:
            future = Future(loop=self._loop)
            self._queue.append(future)
            parent.switch(future)

        self._local.locked = green
        return self.locked()

    def release(self):
        """Release the lock.

        This method should only be called in the locked state;
        it changes the state to unlocked and returns immediately.
        If an attempt is made to release an unlocked lock,
        a RuntimeError will be raised.
        """
        if self._local.locked:
            while self._queue:
                future = self._queue.popleft()
                if not future.done():
                    return future.set_result(None)
            self._local.locked = None
        else:
            raise RuntimeError('release unlocked lock')

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.release()
