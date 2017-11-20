"""Pure python implementation of Pulsar Producer and Protocol

For the cython implementation check the extensions/lib/protocols.pyx module
"""
import time
import asyncio
import logging
from socket import SOL_SOCKET, SO_KEEPALIVE

from .events import EventHandler, AbortEvent


PROTOCOL_LOGGER = logging.getLogger('pulsar.protocols')

CLOSE_TIMEOUT = 3
TIME_INTERVAL = 0.5

dummyRequest = object()


class TimeTracker:
    """Track time in an efficient way
    """
    timeHandlers = {}

    @classmethod
    def register(cls, loop):
        if not cls.timeHandlers.get(loop):
            cls.timeHandlers[loop] = cls(loop)
        return cls.timeHandlers[loop]

    def __init__(self, loop):
        self._loop = loop
        self.handler = None
        self.current_time = None
        self._time()

    def _time(self):
        try:
            self.current_time = int(time.time())
        finally:
            self.handler = self._loop.call_later(TIME_INTERVAL, self._time)


class Producer(EventHandler):
    """An Abstract :class:`.EventHandler` class for all producers of
    sockets (client and servers)
    """
    def __init__(self, protocol_factory, loop=None, name=None,
                 keep_alive=0, logger=None):
        """initialze the Producer

        :param protocol_factory: a callable accepting one parameter only,
            this producer instance and returning a producer protocol
        :param loop: optional event loop
        :param name: optional producer name
        :param keep_alive: optional keep alive timeout for protocols
        :param logger: optional logging instance
        """
        self.protocol_factory = protocol_factory
        self.requests_processed = 0
        self.sessions = 0
        self.keep_alive = keep_alive
        self.logger = logger or PROTOCOL_LOGGER
        self.name = name or self.__class__.__name__
        self._loop = loop or asyncio.get_event_loop()
        self._time = TimeTracker.register(self._loop)

    @property
    def current_time(self):
        return self._time.current_time

    def create_protocol(self):
        """Create a new protocol via the :attr:`protocol_factory`

        This method increase the count of :attr:`sessions` and build
        the protocol passing ``self`` as the producer.
        """
        self.sessions += 1
        protocol = self.protocol_factory(self)
        protocol.copy_many_times_events(self)
        return protocol


class Protocol(EventHandler):
    """A mixin class for both :class:`.Protocol` and
    :class:`.DatagramProtocol`.

    A :class:`.Protocol` is an :class:`.EventHandler` which has
    two :ref:`one time events <one-time-event>`:

    * ``connection_made``
    * ``connection_lost``
    """
    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')

    transport = None
    address = None
    last_change = None
    _current_consumer = None
    _closed = None

    def __init__(self, consumer_factory, producer):
        self.consumer_factory = consumer_factory
        self.producer = producer
        self.session = producer.sessions
        self.processed = 0
        self.data_received_count = 0
        self._loop = producer._loop
        self.event('connection_lost').bind(self._connection_lost)

    def __repr__(self):
        address = self.address
        if address:
            return '%s session %s' % (str(address), self.session)
        else:
            return '<pending> session %s' % self.session
    __str__ = __repr__

    @property
    def closed(self):
        """``True`` if the :attr:`transport` is closed.
        """
        if self.transport:
            return self.transport.is_closing()
        return True

    def current_consumer(self):
        if self._current_consumer is None:
            self._current_consumer = self.consumer_factory(self)
            self._current_consumer.copy_many_times_events(self.producer)
        return self._current_consumer

    def upgrade(self, consumer_factory):
        self.consumer_factory = consumer_factory
        if self._current_consumer is None:
            self.current_consumer()
        else:
            self._current_consumer.event('post_request').bind(
                self._build_consumer
            )

    def connection_made(self, transport):
        """Sets the :attr:`transport`, fire the ``connection_made`` event
        and adds a :attr:`timeout` for idle connections.
        """
        self.transport = transport
        addr = self.transport.get_extra_info('peername')
        if not addr:
            addr = self.transport.get_extra_info('sockname')
        self.address = addr
        sock = transport.get_extra_info('socket')
        if sock:
            try:
                sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
            except (OSError, NameError):
                pass
        self.changed()
        self.producer.logger.debug('new connection %s', self)
        # let everyone know we have a connection with endpoint
        self.event('connection_made').fire()

    def connection_lost(self, exc=None):
        """Fires the ``connection_lost`` event.
        """
        if self._loop.get_debug():
            self.producer.logger.debug('connection lost %s', self)
        self.event('connection_lost').fire(exc=exc)

    def data_received(self, data):
        """Delegates handling of data to the :meth:`current_consumer`.
        Once done set a timeout for idle connections when a
        :attr:`~Protocol.timeout` is a positive number (of seconds).
        """
        try:
            self.data_received_count += 1
            while data:
                consumer = self.current_consumer()
                if not consumer.request:
                    consumer.start()
                toprocess = consumer.feed_data(data)
                consumer.fire_event('data_processed', data=data, exc=None)
                data = toprocess
            self.changed()
        except Exception:
            if self.transport:
                self.transport.abort()
            raise

    def changed(self):
        self.last_change = self.producer.current_time

    def finished_consumer(self, consumer):
        if self._current_consumer is consumer:
            self._current_consumer = None

    # Callbacks
    def _build_consumer(self, _, exc=None):
        self._current_consumer = None
        self.current_consumer()

    def _connection_lost(self, _, exc=None):
        if self._current_consumer:
            self._current_consumer.event('post_request').fire(exc=exc)


class ProtocolConsumer(EventHandler):
    request = None
    ONE_TIME_EVENTS = ('post_request',)

    def __init__(self, connection):
        self.connection = connection
        self.producer = connection.producer
        self._loop = connection._loop

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
        self.event('post_request').bind(self.finished_reading)
        self.request = request or self.create_request()
        try:
            self.fire_event('pre_request')
        except AbortEvent:
            if self._loop.get_debug():
                self.producer.logger.debug('Abort request %s', request)
        else:
            self.start_request()

    def create_request(self):
        return dummyRequest

    def feed_data(self, data):
        """Called when some data is received by the protocol.
        Must be implemented.
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

    def finished_reading(self, *args, **kw):
        """Call this method when the consumer has finished reading
        from the connection
        """
        self.connection.finished_consumer(self)

    def get(self, attr):
        return getattr(self, attr, None)

    def set(self, attr, value):
        setattr(self, attr, value)

    def pop(self, attr, default=None):
        value = getattr(self, attr, default)
        try:
            delattr(self, attr)
        except AttributeError:
            pass
        return value
