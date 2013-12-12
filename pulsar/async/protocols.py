import sys
from functools import partial

from pulsar import TooManyConnections, ProtocolError
from pulsar.utils.internet import nice_address, format_address

from .defer import multi_async, log_failure
from .events import EventHandler
from .internet import Protocol, logger


__all__ = ['Protocol', 'ProtocolConsumer', 'Connection', 'Producer',
           'ConnectionProducer', 'Server']

BIG = 2**31


class ProtocolConsumer(EventHandler):
    '''The consumer of data for a server or client :class:`Connection`.

    It is responsible for receiving incoming data from an end point via the
    :meth:`Connection.data_received` method, decoding (parsing) and,
    possibly, writing back to the client or server via
    the :attr:`transport` attribute.

    .. note::

        For server consumers, :meth:`data_received` is the only method
        to implement.
        For client consumers, :meth:`start_request` should also be implemented.

    A :class:`ProtocolConsumer` is a subclass of :class:`EventHandler` and it
    has two default :ref:`one time events <one-time-event>`:

    * ``pre_request`` fired when the request is received (for servers) or
      just before is sent (for clients).
      This occurs just before the :meth:`start_request` method.
    * ``post_request`` fired when the request is done. The
      :attr:`on_finished` attribute is the
      :class:`Deferred` called back once this event occurs.

    In addition, it has two :ref:`many times events <many-times-event>`:

    * ``data_received`` fired when new data is received from the transport but
      not yet processed (before the :meth:`data_received` method is invoked)
    * ``data_processed`` fired just after data has been consumed (after the
      :meth:`data_received` method)

    .. note::

        A useful example on how to use the ``data_received`` event is
        the :ref:`wsgi proxy server <tutorials-proxy-server>`.
    '''
    _connection = None
    _request = None
    _data_received_count = 0
    ONE_TIME_EVENTS = ('pre_request', 'post_request')
    MANY_TIMES_EVENTS = ('data_received', 'data_processed')

    @property
    def connection(self):
        '''The :class:`Connection` of this consumer.'''
        return self._connection

    @property
    def event_loop(self):
        '''The event loop of this consumer.

        The same as the :attr:`connection` event loop.
        '''
        if self._connection:
            return self._connection.event_loop

    @property
    def request(self):
        ''':class:`Request` instance (used for clients only).'''
        return self._request

    @property
    def transport(self):
        '''The :class:`Transport` of this consumer'''
        if self._connection:
            return self._connection.transport

    @property
    def address(self):
        if self._connection:
            return self._connection.address

    @property
    def producer(self):
        '''The :class:`Producer` of this consumer.'''
        if self._connection:
            return self._connection.producer

    @property
    def on_finished(self):
        '''A :class:`Deferred` called once the request is done.

        A shortcut for ``self.event('post_request')``.
        '''
        return self.event('post_request')

    @property
    def has_finished(self):
        '''``True`` if consumer has finished consuming data.

        This is when the ``finish`` event has been fired.'''
        return self.event('post_request').has_fired()

    def connection_made(self, connection):
        '''Called by a :class:`Connection` when it starts using this consumer.

        By default it does nothing.
        '''

    def data_received(self, data):
        '''Called when some data is received.

        **This method must be implemented by subclasses** for both server and
        client consumers.

        The argument is a bytes object.
        '''

    def start_request(self):
        '''Starts a new request.

        Invoked by the :meth:`start` method to kick start the
        request with remote server. For server :class:`ProtocolConsumer` this
        method is not invoked at all.

        **For clients this method should be implemented** and it is critical
        method where errors caused by stale socket connections can arise.
        **This method should not be called directly.** Use :meth:`start`
        instead. Typically one writes some data from the :attr:`request`
        into the transport. Something like this::

            self.transport.write(self.request.encode())
        '''
        raise NotImplementedError

    def start(self, request=None):
        '''Starts processing the request for this protocol consumer.

        There is no need to override this method,
        implement :meth:`start_request` instead.
        If either :attr:`connection` or :attr:`transport` are missing, a
        :class:`RuntimeError` occurs.

        For server side consumer, this method simply fires the
        ``pre_request`` event with ``request`` as data.'''
        conn = self.connection
        if not conn:
            raise RuntimeError('Cannot start new request. No connection.')
        if not conn.transport:
            raise RuntimeError('%s has no transport.' % conn)
        self._request = request
        self.fire_event('pre_request')
        if self._request is not None:
            try:
                self.start_request()
            except Exception:
                #TODO: should we abort the transport here?
                self.finished(sys.exc_info())

    def finished(self, result=None):
        '''Call this method when done with this :class:`ProtocolConsumer`.

        fires the ``post_request`` event and removes ``self`` from the
        :attr:`connection`.

        :param result: the positional parameter passed to the ``post_request``
            event handler.
        :return: whatever is returned by the ``:meth:`EventHandler.fire_event`
            method (usually ``self`` is the input ``result`` is ``None``,
            otherwise the input ``result``)
        '''
        result = self.fire_event('post_request', result)
        c = self._connection
        if c and c._current_consumer is self:
            c._current_consumer = None
        return result

    def connection_lost(self, exc):
        '''Called by the :attr:`connection` when the transport is closed.

        By default it calls the :meth:`finished` method. It can be overwritten
        to handle the potential exception ``exc``.'''
        log_failure(exc)
        return self.finished(exc)

    def _data_received(self, data):
        # Called by Connection, it updates the counters and invoke
        # the high level data_received method which must be implemented
        # by subclasses
        self._data_received_count = self._data_received_count + 1
        self.fire_event('data_received', data=data)
        result = self.data_received(data)
        self.fire_event('data_processed', data=data)
        return result


class Connection(EventHandler, Protocol):
    '''A :class:`Protocol` which represents a client or server connection
    with an end-point. A :class:`Connection` is not connected until
    :meth:`connection_made` is called by a :class:`Transport`.

    It is a class which acts as bridge between a :class:`SocketTransport`
    and a :class:`ProtocolConsumer`. It routes data arriving from the
    :attr:`transport` to the :attr:`current_consumer`.

    A :class:`Connection` is an :class:`EventHandler` which has
    two :ref:`one time events <one-time-event>`:

    * ``connection_made``
    * ``connection_lost``
    '''
    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')
    #
    _transport = None
    _current_consumer = None
    _idle_timeout = None

    def __init__(self, session, consumer_factory, producer, timeout=0):
        super(Connection, self).__init__()
        self._session = session
        self._processed = 0
        self._timeout = timeout
        self._consumer_factory = consumer_factory
        self._producer = producer

    def __repr__(self):
        address = self.address
        if address:
            return '%s session %s' % (nice_address(address), self._session)
        else:
            return '<pending-connection> session %s' % self._session

    def __str__(self):
        return self.__repr__()

    @property
    def session(self):
        '''Connection session number.

        Passed during initialisation by the :attr:`producer`.
        Usually an integer representing the number of separate connections
        the producer has processed at the time it created this
        :class:`Connection`.
        '''
        return self._session

    @property
    def transport(self):
        '''The :class:`SocketTransport` for this connection.

        Available once the :meth:`connection_made` is called.'''
        return self._transport

    @property
    def sock(self):
        '''The socket of :attr:`transport`.
        '''
        if self._transport:
            return self._transport.sock

    @property
    def event_loop(self):
        '''The :attr:`transport` event loop.'''
        if self._transport:
            return self._transport.event_loop

    @property
    def address(self):
        '''The address of this connection.'''
        if self._transport:
            addr = self._transport.get_extra_info('addr')
            if not addr:
                addr = self._transport.address
            return addr

    @property
    def closed(self):
        '''``True`` if the :attr:`transport` is closed.'''
        return self._transport.closing if self._transport else True

    def close(self, async=True, exc=None):
        '''Close by closing the :attr:`transport`.'''
        if self._transport:
            self._transport.close(async=async, exc=exc)

    def abort(self, exc=None):
        '''Abort by aborting the :attr:`transport`.'''
        if self._transport:
            self._transport.close(async=False, exc=exc)

    @property
    def logger(self):
        '''The python logger for this connection.'''
        return logger(self.event_loop)

    @property
    def consumer_factory(self):
        '''A factory of :class:`ProtocolConsumer` instances.'''
        return self._consumer_factory

    @property
    def current_consumer(self):
        '''The :class:`ProtocolConsumer` currently handling incoming data.

        This instance will receive data when this connection get data
        from the :attr:`transport` via the :meth:`data_received` method.'''
        return self._current_consumer

    @property
    def processed(self):
        '''Number of separate :class:`ProtocolConsumer` processed.

        For connections which are keept alive over several requests.'''
        return self._processed

    @property
    def timeout(self):
        '''Number of seconds to keep alive this connection when an idle.

        A value of ``0`` means no timeout.'''
        return self._timeout

    @property
    def producer(self):
        '''The producer of this :class:`Connection`.

        It is either a :class:`Server` or a client :class:`Client`.'''
        return self._producer

    def set_timeout(self, timeout):
        '''Set a new :attr:`timeout` for this connection.'''
        self._cancel_timeout()
        self._timeout = timeout
        self._add_idle_timeout()

    def set_consumer(self, consumer):
        '''Set a new :class:`ProtocolConsumer` for this :class:`Connection`.

        If the :attr:`current_consumer` is not ``None`` an exception occurs.
        '''
        if consumer is None:
            self._current_consumer = None
        else:
            assert self._current_consumer is None, 'Consumer is not None'
            self._current_consumer = consumer
            consumer._connection = self
            self._processed += 1
            consumer.connection_made(self)

    def connection_made(self, transport):
        '''Override :class:`BaseProtocol.connection_made`.

        Sets the transport, fire the ``connection_made`` event and adds
        a :attr:`timeout` for idle connections.
        '''
        if self._transport is not None:
            self._cancel_timeout()
        self._transport = transport
        # let everyone know we have a connection with endpoint
        self.fire_event('connection_made')
        self._add_idle_timeout()

    def data_received(self, data):
        '''Implements the :meth:`Protocol.data_received` method.

        Delegates handling of data to the :attr:`current_consumer`. Once done
        set a timeout for idle connctions (when a :attr:`timeout` is given).
        '''
        self._cancel_timeout()
        while data:
            consumer = self._current_consumer
            if consumer is None:
                # New consumer.
                consumer = self._consumer_factory()
                self.set_consumer(consumer)
                consumer.start()
            # Call the consumer _data_received method
            data = consumer._data_received(data)
            if data and self._current_consumer:
                # if data is returned from the response feed method and the
                # response has not done yet raise a Protocol Error
                raise ProtocolError('current consumer not done.')
        self._add_idle_timeout()

    def connection_lost(self, exc):
        '''Implements the :meth:`BaseProtocol.connection_lost` method.

        It performs these actions in the following order:

        * Fire the ``connection_lost`` :ref:`one time event <one-time-event>`
          if not fired before, with ``exc`` as event data.
        * Cancel the idle timeout if set.
        * Invokes the :meth:`ProtocolConsumer.connection_lost` method in the
          :attr:`current_consumer` if available.
          '''
        if self.fire_event('connection_lost', exc):
            self._cancel_timeout()
            if self._current_consumer:
                self._current_consumer.connection_lost(exc)
            else:
                log_failure(exc)

    def upgrade(self, consumer_factory=None, build_consumer=False):
        '''Upgrade the :func:`consumer_factory` callable.

        This method can be used when the protocol specification changes
        during a response (an example is a WebSocket request/response,
        or HTTP tunneling). For the upgrade to be successful, the
        ``post_request`` :ref:`event <event-handling>` of the protocol
        consumer should not have been fired already.

        :param consumer_factory: the new consumer factory (a callable
            accepting no parameters)
        :param build_consumer: if ``True`` build the new consumer.
            Default ``False``.
        :return: the new consumer if ``build_consumer`` is ``True``.
        '''
        consumer = self._current_consumer
        if consumer and not consumer.event('post_request').done():
            assert consumer.event('pre_request').done(), "pre_request not done"
            # so that post request won't be fired when the consumer finishes
            consumer.silence_event('post_request')
            self._processed -= 1
            consumer_factory = consumer_factory or self._consumer_factory
            self._consumer_factory = partial(self._upgrade, consumer_factory,
                                             consumer)
            if build_consumer:
                consumer.finished()
                new_consumer = self._consumer_factory()
                self.set_consumer(new_consumer)
                return new_consumer

    ########################################################################
    ##    INTERNALS
    def _timed_out(self):
        self.logger.info(
            '%s idle for %d seconds. Closing connection.', self, self._timeout)
        self.close()

    def _add_idle_timeout(self):
        if not self.closed and not self._idle_timeout and self._timeout:
            self._idle_timeout = self.event_loop.call_later(self._timeout,
                                                            self._timed_out)

    def _cancel_timeout(self):
        if self._idle_timeout:
            self._idle_timeout.cancel()
            self._idle_timeout = None

    def _upgrade(self, consumer_factory, old_consumer):
        # A factory of protocol for an upgrade of an existing protocol consumer
        # which didn't have the post_request event fired.
        consumer = self.producer.build_consumer(consumer_factory)
        consumer.chain_event(old_consumer, 'post_request')
        return consumer


class Producer(EventHandler):
    '''An Abstract :class:`EventHandler` class for all producers of
    connections.
    '''
    connection_factory = Connection
    '''A callable producing connections.

    The signature of the connection factory must be::

        connection_factory(session, consumer_factory, producer, **params)

    By default it is set to the :class:`Connection` class.
    '''
    _timeout = 0
    _max_connections = 0

    def __init__(self, connection_factory=None, timeout=None,
                 max_connections=None):
        super(Producer, self).__init__()
        if connection_factory:
            self.connection_factory = connection_factory
        self._timeout = timeout if timeout is not None else self._timeout
        self._max_connections = max_connections or self._max_connections or BIG

    @property
    def timeout(self):
        '''Number of seconds to keep alive an idle connection.

        Passed as key-valued parameter to to the :meth:`connection_factory`.
        '''
        return self._timeout

    @property
    def max_connections(self):
        '''Maximum number of connections allowed.

        A value of 0 (default) means no limit.
        '''
        return self._max_connections

    def can_reuse_connection(self, connection, response):
        '''Check if ``connection`` can be reused.

        By default it returns ``True``.'''
        return True

    def build_consumer(self, consumer_factory=None):
        '''Build a consumer for a connection.

        **Must be implemented by subclasses.

        :param consumer_factory: optional consumer factory to use.
        '''
        raise NotImplementedError


class ConnectionProducer(Producer):
    '''A Producer of connections with remote servers or clients.

    It is the base class for both :class:`Server` and :class:`ConnectionPool`.
    The main method in this class is :meth:`new_connection` where a new
    connection is created and added to the set of
    :attr:`concurrent_connections`.
    '''
    def __init__(self, **kw):
        super(ConnectionProducer, self).__init__(**kw)
        self._received = 0
        self._concurrent_connections = set()

    @property
    def received(self):
        '''Total number of connections created.'''
        return self._received

    @property
    def concurrent_connections(self):
        '''Number of concurrent active connections.'''
        return len(self._concurrent_connections)

    def new_connection(self, consumer_factory, producer=None):
        '''Called when a new connection is created.

        The ``producer`` is either a :class:`Server` or a :class:`Client`.
        If the number of :attr:`concurrent_connections` is greater or equal
        :attr:`max_connections` a
        :class:`pulsar.utils.exceptions.TooManyConnections` is raised.

        Once a new connection is created, all the many times events of the
        producer are added to the connection.

        :param consumer_factory: The protocol consumer factory passed to the
            :meth:`connection_factory` callable as second positional
            argument.
        :param producer: The producer of the connection. If not specified it
            is set to ``self``. Passed as third positional argument to the
            :meth:`connection_factory` callable.
        :return: the result of the :meth:`connection_factory` call.
        '''
        if self._max_connections and self._received >= self._max_connections:
            raise TooManyConnections('Too many connections')
        # increased the connections counter
        self._received = session = self._received + 1
        # new connection - not yet connected!
        producer = producer or self
        conn = self.connection_factory(session, consumer_factory, producer,
                                       timeout=self.timeout)
        # When the connection is made, add it to the set of
        # concurrent connections
        conn.bind_event('connection_made',
                        partial(self._connection_made, conn))
        conn.copy_many_times_events(producer)
        close = partial(self._connection_lost, conn)
        conn.bind_event('connection_lost', close, close)
        return conn

    def close_connections(self, connection=None, async=True):
        '''Close ``connection`` if specified, otherwise close all connections.

        Return a list of :class:`Deferred` called back once the connection/s
        are closed.
        '''
        all = []
        if connection:
            all.append(connection.event('connection_lost'))
            connection.transport.close(async)
        else:
            for connection in list(self._concurrent_connections):
                all.append(connection.event('connection_lost'))
                connection.transport.close(async)
        if all:
            logger().info('%s closing %d connections', self, len(all))
        return multi_async(all)

    #   INTERNALS
    def _connection_made(self, connection, _):
        self._concurrent_connections.add(connection)
        return _

    def _connection_lost(self, connection, exc):
        # Called when the connection is lost
        self._concurrent_connections.discard(connection)
        return exc


class Server(ConnectionProducer):
    '''A base class for Servers listening on a socket.

    An instance of this class is a :class:`Producer` of server sockets and has
    available two :ref:`one time events <one-time-event>`:

    * ``start`` fired when the server is ready to accept connections.
    * ``stop`` fired when the server has stopped accepting connections. Once a
      a server has stopped, it cannot be reused.

    In addition it has four :ref:`many times event <many-times-event>`:

    * ``connection_made`` fired every time a new :class:`Connection` is made.
    * ``pre_request`` fired every time a new request is made on a
      given connection.
    * ``post_request`` fired every time a request is finished on a
      given connection.
    * ``connection_lost`` fired every time a :class:`Connection` is gone.

    .. attribute:: consumer_factory

        Factory of :class:`ProtocolConsumer` handling the server sockets.
    '''
    ONE_TIME_EVENTS = ('start', 'stop')
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request', 'post_request',
                         'connection_lost')
    consumer_factory = None

    def __init__(self, event_loop, host=None, port=None,
                 consumer_factory=None, name=None, sock=None, **kw):
        super(Server, self).__init__(**kw)
        self._name = name or self.__class__.__name__
        self._event_loop = event_loop
        self._host = host
        self._port = port
        self._sock = sock
        self.logger = logger(event_loop)
        if consumer_factory:
            self.consumer_factory = consumer_factory
        assert hasattr(self.consumer_factory, '__call__'), (
            'consumer_factory must be a callable')

    def __repr__(self):
        return '%s %s' % (self.__class__.__name__,
                          format_address(self.address))
    __str__ = __repr__

    def close(self):
        '''Stop serving and close the listening socket.'''
        raise NotImplementedError

    def protocol_factory(self):
        '''The protocol factory for a server.'''
        return self.new_connection(self.build_consumer)

    def build_consumer(self, consumer_factory=None):
        '''Build a protocol consumer.

        Uses the :meth:`consumer_factory` to build the consumer and add
        events from the many-times events of this producer.

        :return: a protocol consumer.
        '''
        consumer_factory = consumer_factory or self.consumer_factory
        consumer = consumer_factory()
        consumer.copy_many_times_events(self)
        return consumer

    def new_connection(self, consumer_factory, producer=None):
        conn = super(Server, self).new_connection(consumer_factory, producer)
        if self._max_connections and conn._session >= self._max_connections:
            logger().info('Reached maximum number of connections %s. '
                          'Stop serving.' % self._max_connections)
            self.close()
        return conn

    @property
    def event_loop(self):
        '''The :class:`EventLoop` running the server'''
        return self._event_loop

    @property
    def sock(self):
        '''The socket receiving connections.'''
        return self._sock

    @property
    def address(self):
        '''Server address, where clients send requests to.'''
        try:
            return self._sock.getsockname()
        except Exception:
            return None
