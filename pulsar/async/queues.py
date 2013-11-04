from collections import deque

from .defer import CancelledError, Deferred
from .access import asyncio
from .threads import Empty, Full, Lock

__all__ = ['Queue']


class Queue:
    '''Asynchronous FIFO queue.
    '''
    def __init__(self, maxsize=0, loop=None):
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self._lock = Lock()
        self._maxsize = max(maxsize or 0, 0)
        self._queue = deque()
        self._waiting = deque()
        self._putters = deque()

    @property
    def maxsize(self):
        '''Integer representing the upper bound limit on the number of items
that can be placed in the queue.

If :attr:`maxsize` is less than or equal to zero, there is no upper bound.'''
        return self._maxsize

    def qsize(self):
        '''Size of the queue.'''
        return len(self._queue)

    def full(self):
        '''Return True if there are :attr:`maxsize` items in the queue.'''
        return self._maxsize and self.qsize() == self._maxsize

    def put(self, item, timeout=None, wait=True):
        '''Put an ``item`` in the queue.

        If you yield from :meth:`put` and ``timeout`` is ``None``
        (the default), wait until a item is added to the queue. Otherwise
        raise ``Full`` if no slots is available before ``timeout``.

        :param item: item to put into the queue.
        :param timeout: optional timeout in seconds.
        :param wait: optional flag for inserting the item only if one
            slot is immediately available.
        :return: a :class:`Deferred` resulting in ``None`` if ``wait`` is
            ``True``, otherwise ``None``.
        '''
        getter, waiter = None, None
        with self._lock:
            while self._waiting:
                getter = self._waiting.popleft()
                if getter.done():
                    getter = None
                else:
                    break
            # No getter available
            if not getter:
                # no slots not available
                # add it to the putters queue if we can wait
                if self._maxsize and self._maxsize <= self.qsize():
                    if wait:
                        waiter = Deferred(loop=self._loop)
                        waiter.set_timeout(timeout, exception_class=Full)
                        self._putters.append((item, waiter))
                    else:
                        raise Full
                else:
                    # slots available, append to queue
                    self._queue.append(item)
            else:
                assert not self._queue, 'queue non-empty with waiting getters'
        if getter:
            getter.callback(item)
        elif wait and not waiter:
            waiter = Deferred()
            waiter.callback(None)
        return waiter

    def put_nowait(self, item):
        '''Put an item into the queue..

        Put an item if a slot is immediately available, otherwise raise Full.
        Equivalent to ``self.put(item, wait=False)``.
        '''
        return self.put(item, wait=False)

    def get(self, timeout=None, wait=True):
        '''Remove and return an item from the queue.

        If you yield from :meth:`get` and ``timeout`` is ``None``
        (the default), wait until a item is available. Otherwise raise
        ``Empty`` if no item is available before ``timeout``.

        :param timeout: optional timeout in seconds.
        :param wait: optional flag for returning the ``item`` if one is
            immediately available.
        :return: a :class:`Deferred` resulting in the item removed form the
            queue if ``wait`` is ``True``, otherwise the ``item`` removed from
            the queue.
        '''
        with self._lock:
            while self._putters:
                new_item, putter = self._putters.popleft()
                if not putter.done():
                    assert self.full(), 'queue non-full with putters'
                    self._queue.append(new_item)
                    if wait:
                        self._loop.call_soon(putter.callback, None)
                    else:
                        putter.callback(None)
                    break
            if self.qsize():
                item = self._queue.popleft()
                if wait:
                    d = Deferred()
                    d.callback(item)
                    return d
                else:
                    return item
            elif wait:
                item = Deferred(self._loop)
                item.set_timeout(timeout, exception_class=Empty)
                self._waiting.append(item)
                return item
            else:
                raise Empty

    def get_nowait(self):
        '''Remove and return an item from the queue.

        Return an item if one is immediately available otherwise raise Empty.
        Equivalent to ``self.get(wait=False)``.
        '''
        return self.get(wait=False)
