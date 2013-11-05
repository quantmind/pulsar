'''Asynchronous pool for store engines'''
from pulsar import Queue


class Pool(object):
    '''An asynchronous pool of connections.

    Used by :class:`.Store` to maintain a pool of open connections with
    the backend data store.
    Not thread safe. Must be called in the event loop thread.
    '''
    def __init__(self, creator, pool_size=10, timeout=30, loop=None, **kw):
        self._creator = creator
        self._pool_size = pool_size
        self._queue = Queue(loop=loop)
        self._loop = self._queue._loop
        self._in_use_connections = set()

    def connect(self):
        '''Get a connection from the pool.'''
        return PoolConnection.checkout(self)

    @property
    def pool_size(self):
        '''The maximum number of open connections allowed.

        If more connections are requested by a :class:`.Store`, the request
        is queued and a connection returned as soon as one becomes
        available.
        '''
        return self._pool_size

    @property
    def size(self):
        '''The number of connections in use.

        These connections are not available until they are released back
        to the pool.
        '''
        return len(self._in_use_connections)

    def get(self):
        queue = self._queue
        connection = None
        if self.size >= self._pool_size or queue.qsize():
            connection = yield queue.get()
        else:
            connection = yield self._creator()
        self._in_use_connections.add(connection)

    def put(self, conn):
        try:
            self._queue.put(conn, False)
        except pulsar.Full:
            conn.close()
        self._in_use_connections.discard(conn)


class PoolConnection(object):
    __slots__ = ('pool', 'connection')

    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection

    def close(self):
        if self.pool is not None:
            self.pool.put(self.connection)
            self.connection = None
            self.pool = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def __del__(self):
        self.close()

    @classmethod
    def checkout(cls, pool):
        connection = yield pool.get()
        yield cls(pool, connection)
