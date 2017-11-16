import logging
from time import time
import asyncio
from socket import SOL_SOCKET, SO_KEEPALIVE

from clib cimport EventHandler

cdef object dummyRequest = object()
cdef double TIME_INTERVAL = 0.5
cdef PROTOCOL_LOGGER = logging.getLogger('pulsar.protocols')


cdef class TimeTracker:
    timeHandlers = {}

    cdef readonly:
        object _loop, handler
        int current_time

    @classmethod
    def register(cls, loop):
        if not cls.timeHandlers.get(loop):
            cls.timeHandlers[loop] = cls(loop)
        return cls.timeHandlers[loop]

    def __init__(self, loop):
        self._loop = loop
        self._time()

    def _time(self):
        try:
            self.current_time = int(time.time())
        finally:
            self.handler = self._loop.call_later(TIME_INTERVAL, self._time)


cdef class Producer(EventHandler):
    cdef readonly:
        object _loop
        str name
    cdef public:
        int sessions, requests_processed, keep_alive
        object protocol_factory, logger

    def __init__(self, object protocol_factory, object loop=None,
                 str name=None, int keep_alive=0, logger=None):
        self.protocol_factory = protocol_factory
        self.requests_processed = 0
        self.sessions = 0
        self.keep_alive = keep_alive
        self.name = name or self.__class__.__name__
        self.logger = logger or PROTOCOL_LOGGER
        self._loop = loop or asyncio.get_event_loop()
        self.time = TimeTracker.register(self._loop)

    cpdef Protocol create_protocol(self):
        """Create a new protocol via the :meth:`protocol_factory`
        This method increase the count of :attr:`sessions` and build
        the protocol passing ``self`` as the producer.
        """
        self.sessions += 1
        cdef Protocol protocol = self.protocol_factory(self)
        protocol.copy_many_times_events(self)
        return protocol


cdef class Protocol(EventHandler):
    cdef readonly:
        object _loop, consumer_factory, transport, address
        int session, data_received_count, last_change
        Producer producer

    cdef public int processed

    cdef ProtocolConsumer _current_consumer

    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')

    def __init__(self, object consumer_factory, Producer producer):
        self.consumer_factory = consumer_factory
        self.producer = producer
        self.session = producer.sessions
        self.processed = 0
        self.data_received_count = 0
        self._loop = producer._loop
        self.event('connection_lost').bind(self._connection_lost)
        self.changed()

    @property
    def closed(self):
        """``True`` if the :attr:`transport` is closed.
        """
        if self.transport:
            return self.transport.is_closing()
        return True

    cpdef ProtocolConsumer current_consumer(self):
        if self._current_consumer is None:
            self._current_consumer = self.consumer_factory(self)
            self._current_consumer.copy_many_times_events(self.producer)
        return self._current_consumer

    cpdef object upgrade(self, object consumer_factory):
        self.consumer_factory = consumer_factory
        if self._current_consumer is None:
            self.current_consumer()
        else:
            self._current_consumer.event('post_request').bind(self._build_consumer)

    # Asyncio Protocol API
    cpdef connection_made(self, object transport):
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
        # let everyone know we have a connection with endpoint
        self.event('connection_made').fire()

    def connection_lost(self, exc=None):
        """Fires the ``connection_lost`` event.
        """
        self.event('connection_lost').fire()

    cpdef data_received(self, data):
        """Delegates handling of data to the :meth:`current_consumer`.

        Once done set a timeout for idle connections when a
        :attr:`~Protocol.timeout` is a positive number (of seconds).
        """
        cdef ProtocolConsumer consumer;
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

    cpdef int changed(self):
        self.last_change = self.producer.time.current_time
        return self.last_change

    cpdef finished_consumer(self, ProtocolConsumer consumer):
        if self._current_consumer is consumer:
            self._current_consumer = None

    # Callbacks
    cpdef _build_consumer(self, _, exc=None, data=None):
        self._current_consumer = None
        self.current_consumer()

    cpdef _connection_lost(self, _, exc=None, data=None):
        if self._current_consumer:
            event = self._current_consumer.event('post_request')
            if not event.fired():
                event.fire(exc=exc)


cdef class ProtocolConsumer(EventHandler):
    ONE_TIME_EVENTS = ('post_request',)

    cdef readonly:
        Protocol connection
        Producer producer
        object _loop, request

    cdef public:
        dict cache

    def __cinit__(self, Protocol connection):
        self.connection = connection
        self.producer = connection.producer
        self._loop = connection._loop

    cpdef start(self, object request=None):
        self.connection.processed += 1
        self.producer.requests_processed += 1
        self.event('post_request').bind(self._finished)
        self.request = request or self.create_request()
        try:
            self.fire_event('pre_request', data=None, exc=None)
        except AbortEvent:
            if self._loop.get_debug():
                self.producer.logger.debug('Abort request %s', request)
        else:
            self.start_request()

    cpdef object create_request(self):
        return dummyRequest

    cpdef start_request(self):
        pass

    cpdef feed_data(self, data):
        pass

    cpdef finished_reading(self):
        self.connection.finished_consumer(self)

    cpdef _finished(self, object _, object exc=None, object data=None):
        self.connection.finished_consumer(self)

    cpdef object get(self, str attr):
        return getattr(self, attr, None)

    cpdef set(self, str attr, value):
        return setattr(self, attr, value)

    cpdef object pop(self, str attr, object default=None):
        cdef object value = getattr(self, attr, default)
        try:
            delattr(self, attr)
        except AttributeError:
            pass
        return value
