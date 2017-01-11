from time import time
import asyncio
from socket import SOL_SOCKET, SO_KEEPALIVE

from clib cimport EventHandler

cdef object dummyRequest = object()
cdef double TIME_INTERVAL = 0.5


cdef class Producer(EventHandler):
    cdef readonly object protocol_factory
    cdef readonly int sessions
    cdef readonly str name
    cdef readonly object _loop
    cdef public int requests_processed

    def __init__(self, object protocol_factory, object loop=None,
                 str name=None):
        self.protocol_factory = protocol_factory
        self.requests_processed = 0
        self.sessions = 0
        self.name = name or self.__class__.__name__
        self._loop = loop or asyncio.get_event_loop()
        self._time()

    cpdef Protocol create_protocol(self):
        """Create a new protocol via the :meth:`protocol_factory`
        This method increase the count of :attr:`sessions` and build
        the protocol passing ``self`` as the producer.
        """
        self.sessions += 1
        cdef Protocol protocol = self.protocol_factory(self)
        protocol.copy_many_times_events(self)
        return protocol

    cpdef void _time(self):
        try:
            _current_time_ = int(time())
        finally:
            self._loop.call_later(TIME_INTERVAL, self._time)


cdef class Protocol(EventHandler):
    cdef readonly:
        object _loop, consumer_factory, transport, address
        int session, data_received_count, last_change
        Producer producer

    cdef public int processed
    cdef ProtocolConsumer _current_consumer

    def __init__(self, object consumer_factory, Producer producer):
        self.consumer_factory = consumer_factory
        self.producer = producer
        self.session = producer.sessions
        self.processed = 0
        self.data_received_count = 0
        self._loop = producer._loop
        self.event('connection_lost').bind(self._connection_lost)

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
    cpdef void connection_made(self, object transport):
        """Sets the :attr:`transport`, fire the ``connection_made`` event
        and adds a :attr:`timeout` for idle connections.
        """
        self.transport = transport
        addr = self.transport.get_extra_info('peername')
        if not addr:
            addr = self.transport.get_extra_info('sockname')
        self.address = addr
        sock = transport.get_extra_info('socket')
        try:
            sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        except (OSError, NameError):
            pass
        # let everyone know we have a connection with endpoint
        self.event('connection_made').fire()

    cpdef void data_received(self, bytes data):
        """Delegates handling of data to the :meth:`current_consumer`.

        Once done set a timeout for idle connections when a
        :attr:`~Protocol.timeout` is a positive number (of seconds).
        """
        cdef bytes toprocess
        cdef ProtocolConsumer consumer;
        self.data_received_count += 1
        while data:
            consumer = self.current_consumer()
            if not consumer.request:
                consumer.start()
            toprocess = consumer.feed_data(data)
            consumer.fire_event('data_processed', data=data, exc=None)
            data = toprocess
        self.last_change = _current_time_

    cpdef int changed(self):
        self.last_change = _current_time_
        return self.last_change

    # Callbacks
    cpdef void _build_consumer(self, _, exc=None):
        self._current_consumer = None
        self.current_consumer()

    cpdef void _connection_lost(self, _, exc=None):
        if self._current_consumer:
            self._current_consumer.event('post_request').fire(exc=exc)


cdef class ProtocolConsumer(EventHandler):
    ONE_TIME_EVENTS = ('post_request',)

    cdef readonly Protocol connection
    cdef readonly Producer producer
    cdef readonly object _loop
    cdef readonly object request

    def __cinit__(self, EventHandler connection):
        self.connection = connection
        self.producer = connection.producer
        self._loop = connection._loop
        connection._current_consumer = self

    cpdef void start(self, object request=None):
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

    cpdef object create_request(self):
        return dummyRequest

    cpdef void start_request(self):
        pass

    cpdef feed_data(self, bytes data):
        pass

    cpdef void _finished(self, object _, object exc=None):
        self.connection._current_consumer = None
