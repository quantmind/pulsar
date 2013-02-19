import pulsar


class RedisMessageQueue(pulsar.MessageQueue):
    '''A :class:`MessageQueue` based on redis.
This queue is not socket based therefore it requires a specialised IO poller,
the file descriptor is a dummy number and the waker is `self`.
The waker, when invoked via the :meth:`wake`, reduces the poll timeout to 0
so that the :meth:`get` method returns as soon possible.'''
    def __init__(self, client):
        self._client = client
        self._tasks = []
    
    def poller(self, server):
        return RedisQueue(self, server)
    
    def get(self, timeout=0.5):
        '''Get an item from the queue.'''
        t = max(1, min(timeout, 1))
        self.client.blpop(timeout=t).add_callback(self._task, self._error)
        tasks = self._tasks
        self._tasks = []
        return tasks

    def put(self, message):
        self._queue.put(message)
        
    def size(self):
        try:
            return self._queue.qsize()
        except NotImplementedError: #pragma    nocover
            return 0
        
    def fileno(self):
        '''dummy file number'''
        return 1
    
    def close(self, async=False, exc=None):
        pass
    

class RedisQueue(Epoll):
    '''Epoll like class for a IO based on queues rather than sockets.
The interface is the same as the python epoll_ implementation.

.. _epoll: http://docs.python.org/library/select.html#epoll-objects'''
    cpubound = True
    def __init__(self, queue, app):
        super(RedisQueue, self).__init__()
        self._queue = queue
        self._app = app
        self._tasks = []
    
    def poll(self, timeout=1):
        '''Wait for events. timeout in seconds (float)'''
        if self._app.can_poll():
            events = self._queue.get(timeout=timeout)
        else:
            events = []
        events.extend(super(RedisQueue, self).__init__(timeout=timeout))
        return events