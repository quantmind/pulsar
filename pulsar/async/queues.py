from collections import deque

from .defer import get_event_loop, CancelledError, Deferred
from .threads import Empty, Full, Lock

__all__ = ['Queue']

class Queue:
    '''Asynchronous FIFO queue'''
    def __init__(self, maxsize=0, event_loop=None):
        if event_loop:
            self._event_loop = event_loop
        else:
            self._event_loop = get_event_loop()
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
    
    @property
    def event_loop(self):
        '''The event loop running the queue'''
        return self._event_loop
        
    def qsize(self):
        '''Size of the queue.'''
        return len(self._queue)
    
    def full(self):
        '''Return True if there are :attr:`maxsize` items in the queue.'''
        return self._maxsize and self.qsize() == self._maxsize
    
    def put(self, item, timeout=None):
        '''Put an ``item`` in the queue.

If you yield from :meth:`put` and ``timeout`` is ``None`` (the default),
wait until a item is added to the queue. Otherwise raise ``Full`` if no slots
is available before ``timeout``.
        
:parameter timeout: optional timeout in seconds.
:return: a :ref:`coroutine <coroutine>` which results in ``None``.'''
        future, waiter = None, None
        with self._lock:
            while self._waiting:
                future = self._waiting.popleft()
                if future.done():
                    future = None
                else:
                    break
            if not future:
                if self._maxsize and self._maxsize <= self.qsize():
                    waiter = Deferred(event_loop=self._event_loop,
                                      timeout=timeout)
                    self._putters.append((item, waiter))
                else:
                    self._queue.append(item)
            else:
                assert not self._queue, 'queue non-empty with waiting futures'
        if future:
            future.callback(item)
        elif waiter:
            try:
                yield waiter
            except CancelledError:
                raise Full
        
    def get(self, timeout=None):
        '''Remove and return an item from the queue.
        
If you yield from :meth:`get` and ``timeout`` is ``None`` (the default),
wait until a item is available. Otherwise raise ``Empty`` if no item is
available before ``timeout``.
        
:parameter timeout: optional timeout in seconds.
:return: a :ref:`coroutine <coroutine>` which results in the item removed
    form the queue.'''
        with self._lock:
            if self.qsize():
                while self._putters:
                    new_item, putter = self._putters.popleft()
                    if not putter.done():        
                        assert self.full(), 'queue non-full with putters'
                        self._queue.append(new_item)
                        self._event_loop.call_soon(putter.callback, None)
                item = self._queue.popleft()
            else:
                item = Deferred(event_loop=self._event_loop, timeout=timeout)
                self._waiting.append(item)
        try:
            yield item
        except CancelledError:
            raise Empty