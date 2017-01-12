import asyncio
from collections import deque

from async_timeout import timeout

import pulsar

from .access import LOGGER
from .mixins import FlowControl, Timeout, DEFAULT_LIMIT
from ..utils.lib import Protocol, Producer
from ..utils.internet import nice_address, format_address


CLOSE_TIMEOUT = 3


class PulsarProtocol(Protocol, FlowControl, Timeout):
    """A mixin class for both :class:`.Protocol` and
    :class:`.DatagramProtocol`.

    A :class:`PulsarProtocol` is an :class:`.EventHandler` which has
    two :ref:`one time events <one-time-event>`:

    * ``connection_made``
    * ``connection_lost``
    """
    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')

    _transport = None
    _address = None
    _closed = None
    _data_received_count = 0
    last_change = None

    def __init__(self, consumer_factory, producer, limit=None, **kw):
        super().__init__(consumer_factory, producer)
        self.timeout = producer.keep_alive
        self.logger = producer.logger or LOGGER
        self._limit = limit or DEFAULT_LIMIT
        self._b_limit = 2*self._limit
        self._buffer = deque()
        self.event('connection_made').bind(self._set_flow_limits)
        self.event('connection_lost').bind(self._wakeup_waiter)

    def __repr__(self):
        address = self.address
        if address:
            return '%s session %s' % (nice_address(address), self.session)
        else:
            return '<pending> session %s' % self.session
    __str__ = __repr__

    def close(self):
        """Close by closing the :attr:`transport`

        Return the ``connection_lost`` event which can be used to wait
        for complete transport closure.
        """
        if not self._closed:
            closed = False
            event = self.event('connection_lost')
            if self._transport:
                if self._loop.get_debug():
                    self.logger.debug('Closing connection %s', self)
                if self._transport.can_write_eof():
                    try:
                        self._transport.write_eof()
                    except Exception:
                        pass
                try:
                    self._transport.close()
                    closed = self._loop.create_task(
                        self._close(event.waiter())
                    )
                except Exception:
                    pass
            if not closed:
                self.event('connection_lost').fire()
            self._closed = closed or True

    def abort(self):
        """Abort by aborting the :attr:`transport`
        """
        if self._transport:
            self._transport.abort()
        self.event('connection_lost').fire()

    def connection_lost(self, _, exc=None):
        """Fires the ``connection_lost`` event.
        """
        self.event('connection_lost').fire()

    def eof_received(self):
        """The socket was closed from the remote end
        """

    def info(self):
        info = {'connection': {'session': self._session}}
        if self._producer:
            info.update(self._producer.info())
        return info

    async def _close(self, waiter):
        try:
            with timeout(CLOSE_TIMEOUT, loop=self._loop):
                await waiter
        except asyncio.TimeoutError:
            self.logger.warning('Abort connection %s', self)
            self.abort()


class DatagramProtocol(PulsarProtocol, asyncio.DatagramProtocol):
    """An ``asyncio.DatagramProtocol`` with events`
    """


class Connection(PulsarProtocol, asyncio.Protocol):
    """A :class:`.FlowControl` to handle multiple TCP requests/responses.

    It is a class which acts as bridge between a
    :ref:`transport <asyncio-transport>` and a :class:`.ProtocolConsumer`.
    It routes data arriving from the transport to the
    :meth:`current_consumer`.

    .. attribute:: _consumer_factory

        A factory of :class:`.ProtocolConsumer`.

    .. attribute:: _processed

        number of separate requests processed.
    """

    def info(self):
        info = super().info()
        c = info['connection']
        c['request_processed'] = self._processed
        c['data_processed_count'] = self._data_received_count
        c['timeout'] = self.timeout
        return info


class TcpServer(Producer):
    """A :class:`.Producer` of server :class:`Connection` for TCP servers.

    .. attribute:: _server

        A :class:`.Server` managed by this Tcp wrapper.

        Available once the :meth:`start_serving` method has returned.
    """
    ONE_TIME_EVENTS = ('start', 'stop')
    _server = None
    _started = None

    def __init__(self, protocol_factory, *, loop=None, address=None,
                 name=None, sockets=None, max_requests=None,
                 keep_alive=None, logger=None, cfg=None,
                 server_software=None):
        super().__init__(protocol_factory, loop=loop, name=name)
        self.max_requests = max_requests
        self.keep_alive = max(keep_alive or 0, 0)
        self.logger = logger or LOGGER
        self.cfg = cfg
        self.server_software = server_software or pulsar.SERVER_SOFTWARE
        if max_requests:
            self.events('connection_made').bind(self._max_requests)
        self.event('connection_made').bind(self._connection_made)
        self.event('connection_lost').bind(self._connection_lost)
        self._params = {'address': address, 'sockets': sockets}
        self._concurrent_connections = set()

    def __repr__(self):
        address = self.address
        if address:
            return '%s %s' % (self.__class__.__name__, address)
        else:
            return self.__class__.__name__
    __str_ = __repr__

    @property
    def address(self):
        """Socket address of this server.

        It is obtained from the first socket ``getsockname`` method.
        """
        if self._server is not None:
            return self._server.sockets[0].getsockname()

    @property
    def addresses(self):
        return [sock.getsockname() for sock in self.sockets or ()]

    @property
    def sockets(self):
        if self._server is not None:
            return self._server.sockets

    async def start_serving(self, backlog=100, sslcontext=None):
        """Start serving.

        :param backlog: Number of maximum connections
        :param sslcontext: optional SSLContext object.
        :return: a :class:`.Future` called back when the server is
            serving the socket.
        """
        assert not self._server
        if hasattr(self, '_params'):
            address = self._params['address']
            sockets = self._params['sockets']
            del self._params
            create_server = self._loop.create_server
            if sockets:
                server = None
                for sock in sockets:
                    srv = await create_server(self.create_protocol,
                                              sock=sock,
                                              backlog=backlog,
                                              ssl=sslcontext)
                    if server:
                        server.sockets.extend(srv.sockets)
                    else:
                        server = srv
            else:
                if isinstance(address, tuple):
                    server = await create_server(self.create_protocol,
                                                 host=address[0],
                                                 port=address[1],
                                                 backlog=backlog,
                                                 ssl=sslcontext)
                else:
                    raise NotImplementedError
            self._server = server
            self._started = self._loop.time()
            for sock in server.sockets:
                address = sock.getsockname()
                self.logger.info('%s serving on %s', self.name,
                                 format_address(address))
            self._loop.call_soon(self.event('start').fire)

    async def close(self):
        """Stop serving the :attr:`.Server.sockets`.
        """
        if self._server:
            self._server.close()
            self._server = None
            coro = self._close_connections()
            if coro:
                await coro
            self.event('stop').fire()

    def info(self):
        sockets = []
        up = int(self._loop.time() - self._started) if self._started else 0
        server = {'uptime_in_seconds': up,
                  'sockets': sockets,
                  'max_requests': self._max_requests,
                  'keep_alive': self.keep_alive}
        clients = {'processed_clients': self.sessions,
                   'connected_clients': len(self._concurrent_connections),
                   'requests_processed': self._requests_processed}
        if self._server:
            for sock in self._server.sockets:
                sockets.append({
                    'address': format_address(sock.getsockname())})
        return {'server': server,
                'clients': clients}

    #    INTERNALS
    def _connection_made(self, connection, exc=None):
        if not exc:
            self._concurrent_connections.add(connection)

    def _connection_lost(self, connection, exc=None):
        self._concurrent_connections.discard(connection)

    def _max_requests(self, _, exc):
        if (self._server and self.sessions >= self._max_requests):
            self.logger.info('Reached maximum number of connections %s. '
                             'Stop serving.' % self._max_requests)
            self.close()

    def _close_connections(self, connection=None, timeout=5):
        """Close ``connection`` if specified, otherwise close all connections.

        Return a list of :class:`.Future` called back once the connection/s
        are closed.
        """
        all = []
        if connection:
            waiter = connection.event('connection_lost').waiter()
            if waiter:
                all.append(waiter)
                connection.close()
        else:
            connections = list(self._concurrent_connections)
            self._concurrent_connections = set()
            for connection in connections:
                waiter = connection.event('connection_lost').waiter()
                if waiter:
                    all.append(waiter)
                    connection.close()
        if all:
            self.logger.info('%s closing %d connections', self, len(all))
            return asyncio.wait(all, timeout=timeout, loop=self._loop)


class DatagramServer(Producer):
    """An :class:`.Producer` for serving UDP sockets.

    .. attribute:: _transports

        A list of :class:`.DatagramTransport`.

        Available once the :meth:`create_endpoint` method has returned.
    """
    _transports = None
    _started = None

    ONE_TIME_EVENTS = ('start', 'stop')

    def __init__(self, protocol_factory, loop=None, address=None,
                 name=None, sockets=None, max_requests=None,
                 logger=None):
        super().__init__(loop, protocol_factory, name=name,
                         max_requests=max_requests, logger=logger)
        self._params = {'address': address, 'sockets': sockets}

    @property
    def addresses(self):
        return [sock.getsockname() for sock in self.sockets or ()]

    @property
    def sockets(self):
        sockets = []
        if self._transports is not None:
            for t in self._transports:
                sock = t.get_extra_info('socket')
                if sock:
                    sockets.append(sock)
        return sockets

    async def create_endpoint(self, **kw):
        """create the server endpoint.

        :return: a :class:`~asyncio.Future` called back when the server is
            serving the socket.
        """
        if hasattr(self, '_params'):
            address = self._params['address']
            sockets = self._params['sockets']
            del self._params
            transports = []
            loop = self._loop
            if sockets:
                for sock in sockets:
                    transport, _ = await loop.create_datagram_endpoint(
                        self.create_protocol, sock=sock)
                    transports.append(transport)
            else:
                transport, _ = await loop.create_datagram_endpoint(
                    self.create_protocol, local_addr=address)
                transports.append(transport)
            self._transports = transports
            self._started = loop.time()
            for transport in self._transports:
                address = transport.get_extra_info('sockname')
                self.logger.info('%s serving on %s', self.name,
                                 format_address(address))
            self.event('start').fire()

    async def close(self):
        """Stop serving the :attr:`.Server.sockets` and close all
        concurrent connections.
        """
        transports, self._transports = self._transports, None
        if transports:
            for transport in transports:
                transport.close()
            self.event('stop').fire()

    def info(self):
        sockets = []
        up = int(self._loop.time() - self._started) if self._started else 0
        server = {'uptime_in_seconds': up,
                  'sockets': sockets,
                  'max_requests': self._max_requests}
        clients = {'requests_processed': self._requests_processed}
        if self._transports:
            for transport in self._transports:
                sock = transport.get_extra_info('socket')
                if sock:
                    sockets.append({
                        'address': format_address(sock.getsockname())
                    })
        return {'server': server,
                'clients': clients}
