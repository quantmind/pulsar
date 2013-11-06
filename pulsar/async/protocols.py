import sys
from functools import partial

from pulsar import TooManyConnections, ProtocolError
from pulsar.utils.internet import nice_address, format_address

from .defer import multi_async, log_failure, in_loop
from .events import EventHandler
from .access import asyncio, logger


__all__ = ['ProtocolConsumer',
           'Protocol',
           'Connection',
           'Producer',
           'ConnectionProducer',
           'TcpServer']


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
    _data_received_count = 0
    ONE_TIME_EVENTS = ('pre_request', 'post_request')
    MANY_TIMES_EVENTS = ('data_received', 'data_processed')

    @property
    def connection(self):
        '''The :class:`Connection` of this consumer.'''
        return self._connection

    @property
    def _loop(self):
        '''The event loop of this consumer.

        The same as the :attr:`connection` event loop.
        '''
        if self._connection:
            return self._connection._loop

    @property
    def request(self):
        ''':class:`Request` instance (used for clients only).'''
        return getattr(self, '_request', None)

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

        For server side consumer, this method simply fires the ``pre_request``
        event.'''
        if hasattr(self, '_request'):
            raise RuntimeError('Consumer already started')
        conn = self._connection
        if not conn:
            raise RuntimeError('Cannot start new request. No connection.')
        if not conn._transport:
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
            c.current_consumer()
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
        if not hasattr(self, '_request'):
            self.start()
        self._data_received_count = self._data_received_count + 1
        self.fire_event('data_received', data=data)
        result = self.data_received(data)
        self.fire_event('data_processed', data=data)
        return result


class Protocol(EventHandler, asyncio.Protocol):
    '''An ``asyncio.Protocol`` for a :class:`.SocketStreamTransport`.

    A :class:`Protocol` is an :class:`.EventHandler` which has
    two :ref:`one time events <one-time-event>`:

    * ``connection_made``
    * ``connection_lost``
    '''
    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')

    _transport = None
    _current_consumer = None
    _idle_timeout = None

    def __init__(self, session=1, producer=None, timeout=0):
        super(Protocol, self).__init__()
        self._session = session
        self._timeout = timeout
        self._producer = producer

    def __repr__(self):
        address = self.address
        if address:
            return '%s session %s' % (nice_address(address), self._session)
        else:
            return '<pending-connection> session %s' % self._session
    __str__ = __repr__

    @property
    def session(self):
        '''Connection session number.

        Passed during initialisation by the :attr:`producer`.
        Usually an integer representing the number of separate connections
        the producer has processed at the time it created this
        :class:`Protocol`.
        '''
        return self._session

    @property
    def transport(self):
        '''The :class:`.SocketStreamTransport` for this connection.

        Available once the :meth:`connection_made` is called.'''
        return self._transport

    @property
    def sock(self):
        '''The socket of :attr:`transport`.
        '''
        if self._transport:
            return self._transport.sock

    @property
    def address(self):
        '''The address of the :attr:`transport`.
        '''
        if self._transport:
            addr = self._transport.get_extra_info('addr')
            if not addr:
                addr = self._transport.address
            return addr

    @property
    def timeout(self):
        '''Number of seconds to keep alive this connection when idle.

        A value of ``0`` means no timeout.'''
        return self._timeout

    @property
    def _loop(self):
        '''The :attr:`transport` event loop.'''
        if self._transport:
            return self._transport._loop

    @property
    def producer(self):
        '''The producer of this :class:`Protocol`.
        '''
        return self._producer

    @property
    def closed(self):
        '''``True`` if the :attr:`transport` is closed.'''
        return self._transport.closing if self._transport else True

    @property
    def logger(self):
        '''The python logger for this connection.'''
        return logger(self._loop)

    def close(self, async=True, exc=None):
        '''Close by closing the :attr:`transport`.'''
        if self._transport:
            self._transport.close(async=async, exc=exc)

    def abort(self, exc=None):
        '''Abort by aborting the :attr:`transport`.'''
        if self._transport:
            self._transport.close(async=False, exc=exc)

    def connection_made(self, transport):
        '''Sets the transport, fire the ``connection_made`` event and adds
        a :attr:`timeout` for idle connections.
        '''
        if self._transport is not None:
            self._cancel_timeout()
        self._transport = transport
        # let everyone know we have a connection with endpoint
        self.fire_event('connection_made')
        self._add_idle_timeout()

    def connection_lost(self, exc=None):
        '''Fires the ``connection_lost`` event.
        '''
        self.fire_event('connection_lost', exc)

    def set_timeout(self, timeout):
        '''Set a new :attr:`timeout` for this connection.'''
        self._cancel_timeout()
        self._timeout = timeout
        self._add_idle_timeout()

    ########################################################################
    ##    INTERNALS
    def _timed_out(self):
        self.logger.info(
            '%s idle for %d seconds. Closing connection.', self, self._timeout)
        self.close()

    def _add_idle_timeout(self):
        if not self.closed and not self._idle_timeout and self._timeout:
            self._idle_timeout = self._loop.call_later(self._timeout,
                                                       self._timed_out)

    def _cancel_timeout(self):
        if self._idle_timeout:
            self._idle_timeout.cancel()
            self._idle_timeout = None


class Connection(Protocol):
    '''A :class:`Protocol` to handle multiple request/response.

    It is a class which acts as bridge between a
    :class:`.SocketStreamTransport`
    and a :class:`ProtocolConsumer`. It routes data arriving from the
    :class:`.SocketStreamTransport` to the :meth:`current_consumer`.
    '''
    _current_consumer = None

    def __init__(self, consumer_factory, **kw):
        super(Connection, self).__init__(**kw)
        self._processed = 0
        self._consumer_factory = consumer_factory

    def current_consumer(self):
        '''The :class:`ProtocolConsumer` currently handling incoming data.

        This instance will receive data when this connection get data
        from the :attr:`~Protocol.transport` via the :meth:`data_received`
        method.
        '''
        if self._current_consumer is None:
            self._current_consumer = consumer = self._consumer_factory()
            consumer._connection = self
            self._processed += 1
            consumer.connection_made(self)
        return self._current_consumer

    def data_received(self, data):
        '''Delegates handling of data to the :meth:`current_consumer`.

        Once done set a timeout for idle connections when a
        :attr:`~Protocol.timeout` is a positive number (of seconds).
        '''
        self._cancel_timeout()
        while data:
            consumer = self.current_consumer()
            data = consumer._data_received(data)
        self._add_idle_timeout()

    def connection_lost(self, exc):
        '''It performs these actions in the following order:

        * Fires the ``connection_lost`` :ref:`one time event <one-time-event>`
          if not fired before, with ``exc`` as event data.
        * Cancel the idle timeout if set.
        * Invokes the :meth:`ProtocolConsumer.connection_lost` method in the
          :meth:`current_consumer`.
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
                return self._current_consumer


class Producer(EventHandler):
    '''An Abstract :class:`EventHandler` class for all producers of
    connections.
    '''
    protocol_factory = None
    '''A callable producing protocols.

    The signature of the connection factory must be::

        protocol_factory(session, producer, **params)

    By default it is set to the :class:`Connection` class.
    '''
    _timeout = 0
    _max_connections = 0

    def __init__(self, protocol_factory, timeout=None, max_connections=None):
        super(Producer, self).__init__()
        self.protocol_factory = protocol_factory
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

    It is the base class for both :class:`.TcpServer` and
    :class:`ConnectionPool`.
    The main method in this class is :meth:`new_connection` where a new
    connection is created and added to the set of
    :attr:`concurrent_connections`.
    '''
    def __init__(self, protocol_factory, **kw):
        super(ConnectionProducer, self).__init__(protocol_factory, **kw)
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

    def new_connection(self, producer=None):
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


class TcpServer(EventHandler):
    '''A TCP server class.

    .. attribute:: protocol_factory

        Callable for producing :class:`Protocol` to handle the receiving,
        decoding and sending of data.

    .. attribute:: _server

        A :class:`.Server` managed by this Tcp wrapper.

        Available once the :meth:`start_serving` method has returned.
    '''
    ONE_TIME_EVENTS = ('start', 'stop')
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request', 'post_request',
                         'connection_lost')
    _server = None

    def __init__(self, protocol_factory, loop, address=None,
                 name=None, sockets=None, max_connections=None,
                 keep_alive=None):
        super(TcpServer, self).__init__()
        self.protocol_factory = protocol_factory
        self._received = 0
        self._name = name or self.__class__.__name__
        self._loop = loop
        self._params = {'address': address, 'sockets': sockets}
        self._max_connections = max_connections
        self._keep_alive = keep_alive
        self._concurrent_connections = set()
        self.logger = logger(loop)

    @property
    def address(self):
        '''Socket address of this server.

        It is obtained from the first socket ``getsockname`` method.
        '''
        if self._server is not None:
            return self._server.sockets[0].getsockname()

    @in_loop
    def start_serving(self, backlog=100, sslcontext=None):
        '''Start serving.

        :param backlog: Number of maximum connections
        :param sslcontext: optional SSLContext object.
        :return: a :class:`.Deferred` called back when the server is
            serving the socket.'''
        if hasattr(self, '_params'):
            address = self._params['address']
            sockets = self._params['sockets']
            del self._params
            create_server = self._loop.create_server
            try:
                if sockets:
                    server = yield create_server(self._create_protocol,
                                                 sock=sockets,
                                                 backlog=backlog,
                                                 ssl=sslcontext)
                else:
                    if isinstance(address, tuple):
                        server = yield create_server(self._create_protocol,
                                                     host=address[0],
                                                     port=address[1],
                                                     backlog=backlog,
                                                     ssl=sslcontext)
                    else:
                        raise NotImplementedError
                self._server = server
                for sock in server.sockets:
                    address = sock.getsockname()
                    self.logger.info('%s serving on %s', self._name,
                                     format_address(address))
                self.fire_event('start')
            except Exception:
                self.fire_event('start', sys.exc_info())

    def stop_serving(self):
        '''Stop serving the :attr:`.Server.sockets`'''
        if self._server:
            server, self._server = self._server, None
            server.close()
            self.fire_event('stop')
    close = stop_serving

    ##    INTERNALS
    def _create_protocol(self):
        self._received = session = self._received + 1
        protocol = self.protocol_factory(session=session,
                                         producer=self,
                                         timeout=self._keep_alive)
        protocol.bind_event('connection_made', self._connection_made)
        protocol.bind_event('connection_lost',
                            self._connection_lost,
                            partial(self._connection_lost_exc, protocol))
        if self._max_connections and session >= self._max_connections:
            self.stop_serving()
        return protocol

    def _connection_made(self, connection):
        self._concurrent_connections.add(connection)
        return connection

    def _connection_lost(self, connection):
        self._concurrent_connections.discard(connection)
        return connection

    def _connection_lost_exc(self, connection, exc):
        self._concurrent_connections.discard(connection)
        return exc
