import asyncio
import logging
from socket import SOL_SOCKET, SO_KEEPALIVE

from async_timeout import timeout

from ..internet import nice_address

from .events import EventHandler, AbortEvent


LOGGER = logging.getLogger('pulsar.protocols')

CLOSE_TIMEOUT = 3

dummyRequest = object()


class ProtocolConsumer(EventHandler):
    request = None
    ONE_TIME_EVENTS = ('post_request',)

    def __init__(self, connection):
        self.connection = connection
        self.producer = connection.producer
        self._loop = connection._loop
        connection._current_consumer = self

    def finished(self, exc=None):
        """Event fired once a full response to a request is received. It is
        the ``post_request`` one time event.
        """
        self.event('post_request').fire(exc=exc)

    def create_request(self):
        return dummyRequest

    def feed_data(self, data):
        """Called when some data is received.
        """

    def start_request(self):
        """Starts a new request.

        Invoked by the :meth:`start` method to kick start the
        request with remote server. For server :class:`ProtocolConsumer` this
        method is not invoked at all.

        **For clients this method should be implemented** and it is critical
        method where errors caused by stale socket connections can arise.
        **This method should not be called directly.** Use :meth:`start`
        instead. Typically one writes some data from the :attr:`request`
        into the transport. Something like this::

            self.transport.write(self.request.encode())
        """
        pass

    def start(self, request=None):
        """Starts processing the request for this protocol consumer.

        There is no need to override this method,
        implement :meth:`start_request` instead.
        If either :attr:`connection` or :attr:`transport` are missing, a
        :class:`RuntimeError` occurs.

        For server side consumer, this method simply fires the ``pre_request``
        event.
        """
        self.connection.processed += 1
        self.producer.requests_processed += 1
        self.event('post_request').bind(self._finished)
        self.request = request or self.create_request()
        try:
            self.fire_event('pre_request', data=None, exc=None)
        except AbortEvent:
            self.producer.logger.debug('Abort request %s', request)
        else:
            self.start_request()

    def abort_request(self):
        """Abort the request.

        This method can be called during the pre-request stage
        """
        raise AbortEvent

    def _finished(self, _, exc=None):
        c = self.connection
        if c._current_consumer is self:
            c._current_consumer = None


class Protocol(EventHandler):
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

    def __init__(self, consumer_factory, producer):
        self.consumer_factory = consumer_factory
        self.producer = producer
        self._loop = producer._loop

    def __repr__(self):
        address = self._address
        if address:
            return '%s session %s' % (nice_address(address), self.session)
        else:
            return '<pending> session %s' % self.session
    __str__ = __repr__

    @property
    def transport(self):
        """The :ref:`transport <asyncio-transport>` for this protocol.

        Available once the :meth:`connection_made` is called.
        """
        return self._transport

    @property
    def sock(self):
        """The socket of :attr:`transport`.
        """
        if self._transport:
            return self._transport.get_extra_info('socket')

    @property
    def address(self):
        """The address of the :attr:`transport`.
        """
        return self._address

    @property
    def producer(self):
        """The producer of this :class:`Protocol`.
        """
        return self._producer

    @property
    def closed(self):
        """``True`` if the :attr:`transport` is closed.
        """
        if self._transport:
            if hasattr(self._transport, 'is_closing'):
                return self._transport.is_closing()
            return False
        return True

    def current_consumer(self):
        """The :class:`ProtocolConsumer` currently handling incoming data.

        This instance will receive data when this connection get data
        from the :attr:`~PulsarProtocol.transport` via the
        :meth:`data_received` method.

        If no consumer is available, build a new one and return it.
        """
        if self._current_consumer is None:
            consumer = self._consumer_factory(self)
            consumer.copy_many_times_events(self._producer)
        return self._current_consumer

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

    def connection_made(self, transport):
        """Sets the :attr:`transport`, fire the ``connection_made`` event
        and adds a :attr:`timeout` for idle connections.
        """
        self._transport = transport
        addr = self._transport.get_extra_info('peername')
        if not addr:
            addr = self._transport.get_extra_info('sockname')
        self._address = addr
        sock = transport.get_extra_info('socket')
        try:
            sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        except (OSError, NameError):
            pass
        # let everyone know we have a connection with endpoint
        self.event('connection_made').fire()

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


class Producer(EventHandler):
    """An Abstract :class:`.EventHandler` class for all producers of
    socket (client and servers)
    """
    protocol_factory = None
    """A callable producing protocols.

    The signature of the protocol factory callable must be::

        protocol_factory(session, producer, **params)
    """

    def __init__(self, *, loop=None, protocol_factory=None, name=None,
                 max_requests=None, logger=None):
        self.logger = logger or LOGGER
        self._loop = loop or asyncio.get_event_loop()
        self.protocol_factory = protocol_factory or self.protocol_factory
        self._name = name or self.__class__.__name__
        self._requests_processed = 0
        self.sessions = 0
        self._max_requests = max_requests

    @property
    def requests_processed(self):
        """Total number of requests processed.
        """
        return self._requests_processed

    def create_protocol(self, **kw):
        """Create a new protocol via the :meth:`protocol_factory`

        This method increase the count of :attr:`sessions` and build
        the protocol passing ``self`` as the producer.
        """
        self.sessions += 1
        kw['session'] = self.sessions
        kw['producer'] = self
        kw['loop'] = self._loop
        kw['logger'] = self.logger
        return self.protocol_factory(**kw)

    def build_consumer(self, consumer_factory):
        """Build a consumer for a protocol.

        This method can be used by protocols which handle several requests,
        for example the :class:`Connection` class.

        :param consumer_factory: consumer factory to use.
        """
        consumer = consumer_factory(loop=self._loop)
        consumer.logger = self.logger
        consumer.copy_many_times_events(self)
        return consumer
