import logging
from functools import reduce

from pulsar.utils.internet import is_socket_closed

import asyncio

from .futures import AsyncObject
from .protocols import Producer


__all__ = ['Pool', 'PoolConnection', 'AbstractClient', 'AbstractUdpClient']


logger = logging.getLogger('pulsar.pool')


def call_false():
    return False


class Pool(AsyncObject):
    '''An asynchronous pool of open connections.

    Open connections are either :attr:`in_use` or :attr:`available`
    to be used. Available connection are placed in an :class:`asyncio.Queue`.

    This class is not thread safe.
    '''
    def __init__(self, creator, pool_size=10, loop=None, timeout=None, **kw):
        '''
        Construct an asynchronous Pool.

        :param creator: a callable function that returns a connection object.

        :param pool_size: The size of the pool to be maintained,
          defaults to 10. This is the largest number of connections that
          will be kept persistently in the pool. Note that the pool
          begins with no connections; once this number of connections
          is requested, that number of connections will remain.

        :param timeout: The number of seconds to wait before giving up
          on returning a connection. Defaults to 30.
        '''
        self._creator = creator
        self._closed = False
        self._timeout = timeout
        self._queue = asyncio.Queue(maxsize=pool_size, loop=loop)
        self._connecting = 0
        self._loop = self._queue._loop
        self._logger = logger
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

    @property
    def closed(self):
        """True when this pool is closed
        """
        return bool(self._closed)

    def __contains__(self, connection):
        if connection not in self._in_use_connections:
            return connection in self._queue._queue
        return True

    async def connect(self):
        '''Get a connection from the pool.

        The connection is either a new one or retrieved from the
        :attr:`available` connections in the pool.

        :return: a :class:`~asyncio.Future` resulting in the connection.
        '''
        assert not self.closed
        connection = await self._get()
        return PoolConnection(self, connection)

    def close(self):
        '''Close all connections

        Return a :class:`~asyncio.Future` called once all connections
        have closed
        '''
        if not self.closed:
            waiters = []
            queue = self._queue
            while queue.qsize():
                connection = queue.get_nowait()
                if connection:
                    waiters.append(connection.close())
            in_use = self._in_use_connections
            self._in_use_connections = set()
            for connection in in_use:
                if connection:
                    waiters.append(connection.close())
            self._closed = asyncio.gather(*waiters, loop=self._loop)
        return self._closed

    async def _get(self):
        queue = self._queue
        # grab the connection without waiting, important!
        if queue.qsize():
            connection = queue.get_nowait()
        # wait for one to be available
        elif self.in_use + self._connecting >= queue._maxsize:
            connection = await asyncio.wait_for(queue.get(), self._timeout,
                                                loop=self._loop)
        else:   # must create a new connection
            self._connecting += 1
            try:
                connection = await self._creator()
            finally:
                self._connecting -= 1
        # None signal that a connection was removed form the queue
        # Go again
        if connection is None:
            connection = await self._get()
        else:
            if self.is_connection_closed(connection):
                connection = await self._get()
            else:
                self._in_use_connections.add(connection)
        return connection

    def _put(self, conn, discard=False):
        if not self.closed:
            try:
                # None signal that a connection was removed form the queue
                self._queue.put_nowait(None if discard else conn)
            except asyncio.QueueFull:
                # The queue of available connection is already full
                if conn:
                    conn.close()
        self._in_use_connections.discard(conn)

    def is_connection_closed(self, connection):
        if not getattr(connection.transport, 'is_closing', call_false)():
            try:
                sock = connection.sock
            except AttributeError:
                return True
            if is_socket_closed(sock):
                connection.close()
                return True
            return False
        return True

    def status(self, message=None, level=None):
        return ('Pool size: %d  Connections in pool: %d '
                'Current Checked out connections: %d' %
                (self._queue._maxsize, self.available, self.in_use))

    def _count_connections(self, x, y):
        return x + int(y is not None)


class PoolConnection:
    '''A wrapper for a :class:`Connection` in a connection :class:`Pool`.

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
        :attr:`connection` back to the :attr:`pool`.
        '''
        if self.pool is not None:
            self.pool._put(self.connection, discard)
            self.pool = None
            conn, self.connection = self.connection, None
            return conn

    def detach(self, discard=True):
        '''Remove the underlying :attr:`connection` from the connection
        :attr:`pool`.
        '''
        if discard:
            return self.close(True)
        else:
            self.connection._exit_ = False
            return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if getattr(self.connection, '_exit_', True):
            self.close()
        else:
            del self.connection._exit_

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def __setattr__(self, name, value):
        try:
            super().__setattr__(name, value)
        except AttributeError:
            setattr(self.connection, name, value)

    def __del__(self):
        self.close()


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

    async def create_connection(self, address, protocol_factory=None, **kw):
        '''Helper method for creating a connection to an ``address``.
        '''
        protocol_factory = protocol_factory or self.create_protocol
        if isinstance(address, tuple):
            host, port = address
            _, protocol = await self._loop.create_connection(
                protocol_factory, host, port, **kw)
            await protocol.event('connection_made')
        else:
            raise NotImplementedError('Could not connect to %s' %
                                      str(address))
        return protocol


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

    async def create_datagram_endpoint(self, protocol_factory=None, **kw):
        '''Helper method for creating a connection to an ``address``.
        '''
        protocol_factory = protocol_factory or self.create_protocol
        _, protocol = await self._loop.create_datagram_endpoint(
            protocol_factory, **kw)
        await protocol.event('connection_made')
        return protocol
