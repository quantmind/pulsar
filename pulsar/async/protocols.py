import sys
from copy import copy
from functools import partial

from pulsar import TooManyConnections, ProtocolError
from pulsar.utils.internet import nice_address

from .defer import EventHandler, NOTHING, multi_async, log_failure
from .internet import Protocol, logger


__all__ = ['Protocol', 'ProtocolConsumer', 'Connection', 'Producer', 'Server']


class TransportProxy(object):
    '''Provides :class:`Transport` like methods and attributes.'''
    _transport = None
    
    def __repr__(self):
        if self._transport:
            return self._transport.__repr__()
        else:
            return '<closed>'
        
    def __str__(self):
        return self.__repr__()
    
    @property
    def transport(self):
        return self._transport
    
    @property
    def event_loop(self):
        if self._transport:
            return self._transport.event_loop
    
    @property
    def sock(self):
        if self._transport:
            return self._transport.sock
    
    @property
    def address(self):
        if self._transport:
            return self._transport.address
    
    @property
    def closing(self):
        return self._transport.closing if self._transport else True
    
    @property
    def closed(self):
        return self._transport.closed if self._transport else True
    
    def fileno(self):
        if self._transport:
            return self._transport.fileno()
    
    def is_stale(self):
        return self._transport.is_stale() if self._transport else True
    
    def close(self, async=True, exc=None):
        if self._transport:
            self._transport.close(async=async, exc=exc)
        
    def abort(self, exc=None):
        self.close(async=False, exc=exc)
        
        
class ProtocolConsumer(EventHandler):
    '''The protocol consumer is one most important
:ref:`pulsar primitive <pulsar_primitives>`. It is responsible for receiving
incoming data from a the :meth:`Protocol.data_received` method implemented
in :class:`Connection`. It is used to decode and producing responses, i.e.
writing back to the client or server via
the :attr:`transport` attribute. The only method to implement should
be :meth:`Protocol.data_received`.

It has one :ref:`one time events <one-time-event>`:

* ``finish`` fired when this :class:`ProtocolConsumer` has finished consuming
  data and a response/exception is available. The :attr:`on_finished`
  is the :class:`Deferred` called back when this event occurs.

and one :ref:`many times events <many-times-event>`:

* ``data_received`` fired each time new data is received by this
  :class:`ProtocolConsumer` but not yet processed.
* ``data_processed`` fired each time new data is consumed by
  this :class:`ProtocolConsumer`.
'''
    ONE_TIME_EVENTS = ('finish',)
    MANY_TIMES_EVENTS = ('data_received', 'data_processed')
    
    def __init__(self, connection=None):
        super(ProtocolConsumer, self).__init__()
        self._connection = None
        self._current_request = None
        # this counter is updated by the connection
        self._data_received_count = 0
        # this counter is updated via the new_request method
        self._request_processed = 0
        # Number of times the consumer has tried to reconnect (for clients only)
        self._reconnect_retries = 0
        if connection is not None:
            connection.set_consumer(self)
    
    @property
    def connection(self):
        '''The :class:`Connection` of this consumer'''
        return self._connection
    
    @property
    def event_loop(self):
        if self._connection:
            return self._connection.event_loop
    
    @property
    def current_request(self):
        '''Current :class:`Request` instance (used for clients only).'''
        return self._current_request
        
    @property
    def transport(self):
        '''The :class:`Transport` of this consumer'''
        if self._connection:
            return self._connection.transport
        
    @property
    def closed(self):
        '''The :attr:`transport` is closing or it is already closed.'''
        transport = self.transport
        return transport.closing if transport else True
    
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
    def request_processed(self):
        '''The number of requests processed by this consumer.'''
        return self._request_processed
    
    @property
    def on_finished(self):
        '''A :class:`Deferred` called once the :class:`ProtocolConsumer` has
        finished consuming protocol. It is called by the
        :attr:`connection` before disposing of this consumer. It is
        a proxy of ``self.event('finish')``.'''
        return self.event('finish')
    
    def start_request(self):
        '''Invoked by the :meth:`new_request` method to kick start the
request with remote server. For server :class:`ProtocolConsumer` this
method is not invoked at all.

**For clients this method should be implemented** and it is critical method
where errors caused by stale socket connections can arise.
**This method should not be called directly.** Use :meth:`new_request`
instead. Tipically one writes some data from the :attr:`current_request`
into the transport. Something like this::

    self.transport.write(self.current_request.encode())
'''
        pass
    
    def new_request(self, request=None):
        '''Starts a new ``request`` for this protocol consumer. There is
no need to override this method, implement :meth:`start_request` instead.
If either :attr:`connection` or :attr:`transport` are missing, a
:class:`RuntimeError` occurs.

For server side consumer, this method simply add to the
:attr:`request_processed` count and fire the ``pre_request`` event.'''
        conn = self.connection
        if not conn:
            raise RuntimeError('Cannot start new request. No connection.')
        if  not conn.transport:
            raise RuntimeError('%s has no transport.' % conn)
        self._request_processed += 1
        self._current_request = request
        self._connection.fire_event('pre_request', request)
        if request is not None:
            try:
                self.start_request()
            except Exception:
                self.finished(sys.exc_info())
    
    def request_done(self, exc=None):
        '''Call this method when done with the :attr:`current_request`.'''
        if self._connection:
            self._connection.fire_event('post_request', self)        
    
    def data_received(self, data):
        '''Called when some data is received.

        The argument is a bytes object.'''
        pass
        
    def reset_connection(self):
        '''Cleanly dispose of the current :attr:`connection`. Used
by client consumers only.'''
        if self._connection:
            conn = self._connection
            clone = copy(self)
            conn._current_consumer = clone
            self._connection = None
            clone.finished()
            return clone
        
    def finished(self, result=NOTHING):
        '''Call this method when done with this :class:`ProtocolConsumer`.
        
If a :attr:`connection` is available, fire the connection ``post_request``
event and set :attr:`connection` to ``None``. Finally fire the ``finish``
event with ``result`` as argument. Return ``result``.'''
        c = self._connection
        if c:
            c._current_consumer = None
            c.fire_event('post_request', self)
        self.fire_event('finish', result)
        self._connection = None
        return result
        
    def connection_lost(self, exc):
        '''Called by the :attr:`connection` when the transport is closed.
        
        By default it calls the :meth:`finished` method. It can be overwritten
        to handle the potential exception ``exc``.'''
        log_failure(exc)
        return self.finished(exc)
        
    def can_reconnect(self, max_reconnect, exc):
        conn = self._connection
        # First we check if this was caused by a stale connection
        if conn and not self._data_received_count and conn.processed > 1:
            # switch off logging for this exception
            exc.logged = True
            return 1
        elif self._reconnect_retries < max_reconnect:
            self._reconnect_retries += 1
            exc.log()
            return self._reconnect_retries
        else:
            return 0
        
    def _data_received(self, data):
        # Called by Connection, it updates the counters and invoke
        # the high level data_received method which must be implemented
        # by subclasses
        self._data_received_count += 1 
        self._reconnect_retries = 0
        self.fire_event('data_received', data)
        result = self.data_received(data)
        self.fire_event('data_processed')
        return result
        
        
class Connection(EventHandler, Protocol, TransportProxy):
    '''A :class:`Protocol` which represents a client or server connection
with an end-point. This is not connected until
:meth:`connection_made` is called by the :class:`Transport`.
This is the bridge between the :class:`Transport`
and the :class:`ProtocolConsumer`. It has a :class:`Protocol`
interface and it routes data arriving from the :attr:`transport` to
the :attr:`current_consumer`, an instance of :class:`ProtocolConsumer`.

A :class:`Connection` is an :class:`EventHandler` which has
two :ref:`one time events <one-time-event>`:

* ``connection_made``
* ``connection_lost``

and two :ref:`many times events <many-times-event>`:

* ``pre_request``
* ``post_request``

.. attribute:: producer

    The producer of this :class:`Connection`, It is either a :class:`Server`
    or a client :class:`Client`.
    
.. attribute:: transport

    The :class:`Transport` of this protocol connection. Initialised once the
    :meth:`Protocol.connection_made` is called.
    
.. attribute:: processed

    Number of separate :class:`ProtocolConsumer` processed by this connection.
    
.. attribute:: current_consumer

    The :class:`ProtocolConsumer` currently handling incoming data.
'''
    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')
    MANY_TIMES_EVENTS = ('data_received', 'pre_request', 'post_request')
    #
    def __init__(self, session, timeout, consumer_factory, producer):
        super(Connection, self).__init__()
        self._session = session 
        self._processed = 0
        self._timeout = timeout
        self._idle_timeout = None
        self._current_consumer = None
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
        '''Connection session number. Created by the :attr:`producer`.'''
        return self._session
    
    @property
    def address(self):
        try:            
            return self._transport._extra['addr']
        except Exception:
            return None
        
    @property
    def logger(self):
        '''The python logger for this connection.'''
        return logger(self.event_loop)
    
    @property
    def consumer_factory(self):
        '''A factory of :class:`ProtocolConsumer` instances for this
        :class:`Connection`.'''
        return self._consumer_factory
    
    @property
    def current_consumer(self):
        return self._current_consumer
        
    @property
    def processed(self):
        return self._processed
    
    @property
    def timeout(self):
        '''Number of seconds to keep alive this connection when an idle.'''
        return self._timeout
    
    @property
    def producer(self):
        return self._producer
    
    def set_timeout(self, timeout):
        self._cancel_timeout()
        self._timeout = timeout
        self._add_idle_timeout()
        
    def set_consumer(self, consumer):
        '''Set a new :class:`ProtocolConsumer` for this :class:`Connection`.
If the :attr:`current_consumer` is not ``None`` an exception occurs'''
        assert self._current_consumer is None, 'Consumer is not None'
        self._current_consumer = consumer
        consumer._connection = self
        self._processed += 1
    
    def connection_made(self, transport):
        '''Override :class:`BaseProtocol.connection_made` by setting
the transport, firing the ``connection_made`` event and adding a timeout
for idel connections.'''
        # Implements protocol connection_made
        self._transport = transport
        # let everyone know we have a connection with endpoint
        self.fire_event('connection_made')
        self._add_idle_timeout()
        
    def data_received(self, data):
        '''Implements the :meth:`Protocol.data_received` method.
        
        Delegates handling of data to the :attr:`current_consumer`. Once done
        set a timeout for idle connctions (when a :attr:`timeout` is given).'''
        self._cancel_timeout()
        while data:
            consumer = self._current_consumer
            if consumer is None:
                # New consumer.
                # These two lines are used by server connections only.
                consumer = self._consumer_factory(self)
                consumer.new_request()
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
  :attr:`current_consumer` if available.'''
        if self.fire_event('connection_lost', exc):
            self._cancel_timeout()
            if self._current_consumer:
                self._current_consumer.connection_lost(exc)
            else:
                log_failure(exc)
                             
    def upgrade(self, consumer_factory):
        '''Update the :attr:`consumer_factory` attribute with a new
:class:`ProtocolConsumer` factory. This function can be used when the protocol
specification changes during a response (an example is a WebSocket
response).'''
        self._consumer_factory = consumer_factory
    
    ############################################################################
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
         
         
class Producer(EventHandler):
    '''A Producer of :class:`Connection` with remote servers or clients.
It is the base class for both :class:`Server` and :class:`ConnectionPool`.
The main method in this class is :meth:`new_connection` where a new
:class:`Connection` is created and added to the set of
:attr:`concurrent_connections`.

.. attribute:: connection_factory

    A factory producing the :class:`Connection` from a
    remote client with this producer.
    This attribute is used in the :meth:`new_connection` method.
    There shouldn't be any reason to change the default :class:`Connection`,
    it is here just in case.
    
.. attribute:: concurrent_connections

    Number of concurrent active connections
    
.. attribute:: received

    Total number of received connections
    
.. attribute:: timeout

    number of seconds to keep alive an idle connection
    
.. attribute:: max_connections

    Maximum number of connections allowed. A value of 0 (default)
    means no limit.
'''
    connection_factory = Connection
    def __init__(self, max_connections=0, timeout=0, connection_factory=None):
        super(Producer, self).__init__()
        self._received = 0
        self._timeout = timeout
        self._max_connections = max_connections
        self._concurrent_connections = set()
        if connection_factory:
            self.connection_factory = connection_factory
    
    @property
    def timeout(self):
        return self._timeout
    
    @property
    def received(self):
        return self._received
    
    @property
    def max_connections(self):
        return self._max_connections
    
    @property
    def concurrent_connections(self):
        return len(self._concurrent_connections)
    
    def new_connection(self, consumer_factory, producer=None):
        '''Called when a new :class:`Connection` is created. The ``producer``
is either a :class:`Server` or a :class:`Client`. If the number of
:attr:`concurrent_connections` is greater or equal :attr:`max_connections`
a :class:`pulsar.utils.exceptions.TooManyConnections` is raised.'''
        if self._max_connections and self._received >= self._max_connections:
            raise TooManyConnections('Too many connections')
        # increased the connections counter
        self._received = session = self._received + 1
        # new connection - not yet connected!
        producer = producer or self
        conn = self.connection_factory(session, self.timeout,
                                       consumer_factory, producer)
        # When the connection is made, add it to the set of
        # concurrent connections
        conn.bind_event('connection_made', self._add_connection)
        conn.copy_many_times_events(producer)
        conn.bind_event('connection_lost', partial(self._remove_connection,
                                                   conn))
        return conn
    
    def close_connections(self, connection=None, async=True):
        '''Close ``connection`` if specified, otherwise close all
active connections. Return a list of :class:`Deferred` called
back once the connection/s are closed.'''
        all = []
        if connection:
            all.append(connection.event('connection_lost'))
            connection.transport.close(async)
        else:
            for connection in list(self._concurrent_connections):
                all.append(connection.event('connection_lost'))
                connection.transport.close(async)
        return multi_async(all)
                
    def can_reuse_connection(self, connection, response):
        return True
            
    def _add_connection(self, connection):
        self._concurrent_connections.add(connection)
        
    def _remove_connection(self, connection, exc=None):
        # Called when the connection is lost
        self._concurrent_connections.discard(connection)
    
    
class Server(Producer):
    '''A base class for Servers listening on a socket.
    
An instance of this class is a :class:`Producer` of server sockets and has
available two :ref:`one time events <one-time-event>`:

* ``start`` fired when the server is ready to accept connections.
* ``stop`` fired when the server has stopped accepting connections. Once a
  a server has stopped, it cannot be reused.
  
In addition it has four :ref:`many times event <many-times-event>`:

* ``connection_made`` fired every time a new connection is made.
* ``pre_request`` fired every time a new request is made on a given connection.
* ``post_request`` fired every time a request is finished on a given connection.
* ``connection_lost`` fired every time a connection is gone.

.. attribute:: consumer_factory

    Factory of :class:`ProtocolConsumer` handling the server sockets.
    '''
    ONE_TIME_EVENTS = ('start', 'stop')
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request','post_request',
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
    
    def close(self):
        '''Stop serving and close the listening socket.'''
        raise NotImplementedError
    
    def protocol_factory(self):
        return self.new_connection(self.consumer_factory)
        
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
        return self._sock.getsockname()
        
