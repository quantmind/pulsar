from pulsar.utils.internet import is_socket_closed

from .defer import coroutine_return
from .events import EventHandler
from .queues import Queue, Full


__all__ = ['Pool', 'AbstractClient']


class Pool(object):
    '''An asynchronous pool of open connections.

    Open connections are either :attr:`in_use` or :attr:`available`
    to be used. Available connection are placed in an
    asynchronous  :class:`.Queue`.

    This class is not thread safe.
    '''
    def __init__(self, creator, pool_size=10, loop=None, timeout=None, **kw):
        self._creator = creator
        self._closed = False
        self._timeout = timeout
        self._queue = Queue(loop=loop, maxsize=pool_size)
        self._loop = self._queue._loop
        self._waiting = 0
        self._in_use_connections = set()

    @property
    def pool_size(self):
        '''The maximum number of open connections allowed.

        If more connections are requested, the request
        is queued and a connection returned as soon as one becomes
        available.
        '''
        return self._queue._maxsize

    @property
    def in_use(self):
        '''The number of connections in use.

        These connections are not available until they are released back
        to the pool.
        '''
        return len(self._in_use_connections) + self._waiting

    @property
    def available(self):
        '''Number of available connections in the pool.
        '''
        return self._queue.qsize()

    def connect(self):
        '''Get a connection from the pool.

        The connection is either a new one or retrieved from the
        :attr:`available` connections in the pool.

        :return: a :class:`.Deferred` resulting in the connection.
        '''
        assert not self._closed
        return PoolConnection.checkout(self)

    def close(self):
        '''Close all :attr:`available` and :attr:`in_use` connections.
        '''
        self._closed = True
        queue = self._queue
        while queue.qsize():
            connection = queue.get_nowait()
            connection.close()
        in_use = self._in_use_connections
        self._in_use_connections = set()
        for connection in in_use:
            connection.close()

    def _get(self):
        queue = self._queue
        connection = None
        # all available connections are in use or there are some available
        if self.in_use >= self._queue._maxsize or self._queue.qsize():
            connection = yield queue.get(timeout=self._timeout)
            if is_socket_closed(connection.sock):
                if connection._transport:
                    connection._transport.close()
                connection = yield self._get()
            else:
                self._in_use_connections.add(connection)
        else:
            self._waiting += 1
            connection = yield self._creator()
            self._in_use_connections.add(connection)
            self._waiting -= 1
        coroutine_return(connection)

    def _put(self, conn):
        if not self._closed:
            try:
                self._queue.put_nowait(conn)
            except Full:
                conn.close()
        self._in_use_connections.discard(conn)

    def info(self, message=None, level=None):   # pragma    nocover
        if self._queue._maxsize != 2:
            return
        message = '%s: ' % message if message else ''
        self._loop.logger.log(level or 10,
                              '%smax size %s, in_use %s, available %s',
                              message, self._queue._maxsize, self.in_use,
                              self.available)


class PoolConnection(object):
    __slots__ = ('pool', 'connection')

    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection

    def close(self):
        if self.pool is not None:
            self.pool._put(self.connection)
            self.pool = None
            self.connection = None

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
        connection = yield pool._get()
        yield cls(pool, connection)


class AbstractClient(EventHandler):
    '''A client for a remote server.
    '''
    ONE_TIME_EVENTS = ('finish',)

    def __init__(self, loop):
        super(AbstractClient, self).__init__()
        self._loop = loop

    def __repr__(self):
        return self.__class__.__name__
    __str__ = __repr__

    def connect(self):
        '''Abstract method for creating a server connection.
        '''
        raise NotImplementedError

    def request(self, *args, **params):
        '''Abstract method for creating a :class:`Request`.
        '''
        raise NotImplementedError

    def close(self, async=True, timeout=5):
        ''':meth:`close` all idle connections but wait for active connections
        to finish.
        '''
        return self.fire_event('finish')

    def abort(self):
        ''':meth:`close` all connections without waiting for active
        connections to finish.
        '''
        return self.close(async=False)

    def create_connection(self, protocol_factory, address, **kw):
        if isinstance(address, tuple):
            host, port = address
            _, connection = yield self._loop.create_connection(
                protocol_factory, host, port, **kw)
        else:
            raise NotImplementedError
        coroutine_return(connection)
