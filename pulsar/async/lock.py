import asyncio
from abc import ABC, abstractmethod

from ..utils.exceptions import LockError


class LockBase(ABC):
    """A asynchronous locking primitive associated to a given name.

    An asynchronous lock is in one of two states, 'locked' or 'unlocked'.
    It is created in the unlocked state. It has two basic methods,
    :meth:`.acquire` and :meth:`.release. When the state is unlocked,
    :meth:`.acquire` changes the state to locked and returns immediately.

    When the state is locked, :meth:`.acquire` wait
    until a call to :meth:`.release` changes it to unlocked,
    then the :meth:`.acquire` call resets it to locked and returns.

    .. attribute:: blocking

        The time to wait for the lock to be free when acquiring it.
        When False it does not block, when True it blocks forever,
        when a positive number blocks for ``blocking`` seconds.

    .. attribute:: timeout

        Free the lock after timeout seconds. If timeout is None (default)
        does not free the lock until ``release`` is called.
    """
    def __init__(self, name, *, loop=None, timeout=None, blocking=True):
        self.name = name
        self.timeout = timeout
        self.blocking = blocking
        self._loop = loop or asyncio.get_event_loop()

    @abstractmethod
    def locked(self):   # pragma    nocover
        """Return True if the lock is acquired
        """
        raise

    @abstractmethod
    async def acquire(self):    # pragma    nocover
        """Try to acquire the lock
        """
        raise

    @abstractmethod
    async def release(self):    # pragma    nocover
        """Release the lock
        """
        raise

    async def __aenter__(self):
        acquired = await self.acquire()
        if not acquired:
            raise LockError('Could not acquire lock')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.locked():
            await self.release()


class Lock(LockBase):
    """An asynchronous lock
    """
    def __init__(self, name, *, loop=None, timeout=None, blocking=True):
        super().__init__(name, loop=loop, timeout=timeout, blocking=blocking)
        self._lock = _get_lock(self._loop, name)
        self._timeout_handler = None
        self._locked = False

    def locked(self):
        return self._locked

    async def acquire(self):
        try:
            timeout = self.blocking
            if timeout is True:
                timeout = None
            elif timeout is False:
                timeout = 0
            await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
            self._schedule_timeout()
            self._locked = True
        except asyncio.TimeoutError:
            self._locked = False
        return self._locked

    async def release(self):
        self._cancel_lock()

    def _schedule_timeout(self):
        if self.timeout is not None:
            self._timeout_handler = self._loop.call_later(
                self.timeout, self._cancel_lock
            )

    def _cancel_lock(self):
        try:
            self._lock.release()
        except Exception:
            pass
        self._locked = False
        if self._timeout_handler:
            self._timeout_handler.cancel()
            self._timeout_handler = None


def _get_lock(loop, name):
    if not hasattr(loop, '_locks'):
        loop._locks = {}

    if name not in loop._locks:
        loop._locks[name] = asyncio.Lock(loop=loop)

    return loop._locks[name]
