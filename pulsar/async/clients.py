from functools import reduce

from pulsar.utils.internet import is_socket_closed

from .access import asyncio
from .futures import coroutine_return, AsyncObject, future_timeout
from .protocols import Producer


__all__ = ['Pool', 'PoolConnection', 'AbstractClient', 'AbstractUdpClient']


class Pool(AsyncObject):
    '''An asynchronous pool of open connections.

    Open connections are either :attr:`in_use` or :attr:`available`
    to be used. Available connection are placed in an :class:`asyncio.Queue`.

    This class is not thread safe.
    '''
    def __init__(self, creator, pool_size=10, loop=None, timeout=None, **kw):
        self._creator = creator
        self._closed = False
        self._timeout = timeout
        self._queue = asyncio.Queue(maxsize=pool_size, loop=loop)
        self._connecting = 0
        self._loop = self._queue._loop
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
        return len(self._in_use_connections)

    @property
    def available(self):
        '''Number of available connections in the pool.
        '''
        return reduce(self._count_connections, self._queue._queue, 0)

    def __contains__(self, connection):
        if connection not in self._in_use_connections:
            return connection in self._queue._queue
        return True

    def connect(self):
        '''Get a connection from the pool.

        The connection is either a new one or retrieved from the
        :attr:`available` connections in the pool.

        :return: a :class:`~asyncio.Future` resulting in the connection.
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
        # grab the connection without waiting, important!
        if queue.qsize():
            connection = queue.get_nowait()
        # wait for one to be available
        elif self.in_use + self._connecting >= queue._maxsize:
            if self._timeout:
                connection = yield future_timeout(queue.get(), self._timeout)
            else:
                connection = yield queue.get()
        else:   # must create a new connection
            self._connecting += 1
            try:
                connection = yield self._creator()
            finally:
                self._connecting -= 1
        # None signal that a connection was removed form the queue
        # Go again
        if connection is None:
            connection = yield self._get()
        else:
            if self.is_connection_closed(connection):
                connection = yield self._get()
            else:
                self._in_use_connections.add(connection)
        coroutine_return(connection)

    def _put(self, conn, discard=False):
        if not self._closed:
            try:
                self._queue.put_nowait(None if discard else conn)
            except asyncio.QueueFull:
                conn.close()
        self._in_use_connections.discard(conn)

    def is_connection_closed(self, connection):
        if is_socket_closed(connection.sock):
            connection.close()
            return True
        return False

    def info(self, message=None, level=None):   # pragma    nocover
        if self._queue._maxsize != 2:
            return
        message = '%s: ' % message if message else ''
        self.logger.log(level or 10,
                        '%smax size %s, in_use %s, available %s',
                        message, self._queue._maxsize, self.in_use,
                        self.available)

    def _count_connections(self, x, y):
        return x + int(y is not None)


class PoolConnection(object):
    '''A wrapper for a :class:`Connection` in a connection :class:`Pool`.

    Objects are never initialised directly, instead they are `checked-out`
    via the :meth:`checkout` class method from the :meth:`Pool.connect`
    method.

    .. attribute:: pool

        The :class:`Pool` which created this :class:`PoolConnection`

    .. attribute:: connection

        The underlying socket connection.
    '''
    __slots__ = ('pool', 'connection')

    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection

    def close(self, discard=False):
        '''Close this pool connection by releasing the underlying
        :attr:`connection` back to the ;attr:`pool`.
        '''
        if self.pool is not None:
            self.pool._put(self.connection, discard)
            self.pool = None
            self.connection = None

    def detach(self):
        '''Remove the underlying :attr:`connection` from the connection
        :attr:`pool`.
        '''
        self.close(True)

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
        '''Checkout a new connection from ``pool``.
        '''
        connection = yield pool._get()
        coroutine_return(cls(pool, connection))


class AbstractClient(Producer):
    '''A :class:`.Producer` for a client connections.
    '''
    ONE_TIME_EVENTS = ('finish',)

    def __repr__(self):
        return self.__class__.__name__
    __str__ = __repr__

    def connect(self):
        '''Abstract method for creating a connection.
        '''
        raise NotImplementedError

    def close(self):
        '''Close all idle connections.
        '''
        return self.fire_event('finish')
    abort = close

    def create_connection(self, address, protocol_factory=None, **kw):
        '''Helper method for creating a connection to an ``address``.
        '''
        protocol_factory = protocol_factory or self.create_protocol
        if isinstance(address, tuple):
            host, port = address
            _, protocol = yield self._loop.create_connection(
                protocol_factory, host, port, **kw)
        else:
            raise NotImplementedError('Could not connect to %s' %
                                      str(address))
        coroutine_return(protocol)


class AbstractUdpClient(Producer):
    '''A :class:`.Producer` for a client udp connections.
    '''
    ONE_TIME_EVENTS = ('finish',)

    def __repr__(self):
        return self.__class__.__name__
    __str__ = __repr__

    def create_endpoint(self):
        '''Abstract method for creating the endpoint
        '''
        raise NotImplementedError

    def close(self):
        '''Close all idle connections.
        '''
        return self.fire_event('finish')
    abort = close

    def create_datagram_endpoint(self, protocol_factory=None, **kw):
        '''Helper method for creating a connection to an ``address``.
        '''
        protocol_factory = protocol_factory or self.create_protocol
        _, protocol = yield self._loop.create_datagram_endpoint(
            protocol_factory, **kw)
        yield protocol.event('connection_made')
        coroutine_return(protocol)
