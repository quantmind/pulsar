import sys
from copy import copy
from functools import partial

from pulsar import ProtocolError
from pulsar.utils.sockets import nice_address
from pulsar.async.defer import EventHandler, NOTHING

from .transport import TransportProxy, Connector, LOGGER


__all__ = ['Protocol', 'ProtocolConsumer', 'Connection', 'Producer']


class Protocol(EventHandler):
    '''Abstract class implemented in :class:`Connection`
and :class:`ProtocolConsumer`. It derives from :class:`EventHandler`
and therefore several events can be attached to subclasses.'''
    def connection_made(self, transport):
        '''Indicates that the :class:`Transport` is ready and connected
to the entity at the other end. The protocol should probably save the
transport reference as an instance variable (so it can call its write()
and other methods later), and may write an initial greeting or request
at this point.'''
        raise NotImplementedError
    
    def data_received(self, data):
        '''The :class:`Transport` has read some data from *other end*
and it invokes this method.'''
        raise NotImplementedError
    
    def eof_received(self):
        '''This is called when the other end called write_eof() (or
something equivalent).'''
        raise NotImplementedError
        
    def connection_lost(self, exc):
        '''The :class:`Transport` has been closed or aborted, has detected
that the other end has closed the connection cleanly, or has encountered an
unexpected error. In the first three cases the argument is None;
for an unexpected error, the argument is the exception that caused
the transport to give up.'''
        pass

    
class ProtocolConsumer(Protocol):
    '''The :class:`Protocol` consumer is one most important
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

.. attribute:: connection

    The :class:`Connection` of this consumer
    
.. attribute:: current_request

    Current request instance (used for clients only).
    
.. attribute:: connecting

    ``True`` if connecting to endpoint (for servers this is always false).
    
.. attribute:: connected

    ``True`` if an end-point connection is available.
    
.. attribute:: producer

    The :class:`Producer` of this consumer.
    
.. attribute:: on_finished

    A :class:`Deferred` called once the :class:`ProtocolConsumer` has
    finished consuming protocol. It is called by the
    :attr:`connection` before disposing of this consumer. It is
    a proxy of ``self.event('finish')``.

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
        return self._connection
    
    @property
    def connected(self):
        return isinstance(self._connection, Connection)
    
    @property
    def connecting(self):
        return isinstance(self._connection, Connector)
    
    @property
    def event_loop(self):
        if self._connection:
            return self._connection.event_loop
    
    @property
    def current_request(self):
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
        if self._connection:
            return self._connection.producer
    
    @property
    def request_processed(self):
        '''The number of requests processed by this consumer.'''
        return self._request_processed
    
    @property
    def on_finished(self):
        return self.event('finish')
    
    def start_request(self):
        '''Invoked by the :meth:`new_request` method to kick start the
request with remote server/client. For server :class:`ProtocolConsumer` this
method is usually not implemented and therefore is simply a pass-through.
**For clients this method should be implemented** and it is critical method
where errors caused by stale socket connections can arise.
**This method should not be called directly.** Use :meth:`new_request`
instead. Tipically one writes some data from the :attr:`current_request`
into the transport. Something like this::

    self.transport.write(self.current_request.encode())
'''
        pass
    
    def new_request(self, request=None):
        '''Starts a new ``request`` for this protocol consumer if
:attr:`connected` or :attr:`connecting` is `True`. There is no need to
override this method, implement :meth:`start_request` instead.'''
        if self.connected:
            self._request_processed += 1
            self._current_request = request
            self._connection.fire_event('pre_request', request)
            if request is not None:
                try:
                    self.start_request()
                except Exception:
                    self.finished(sys.exc_info())
        elif self.connecting:
            self._connection.add_callback(lambda r: self.new_request(request))
        else:
            raise RuntimeError('Cannot start new request. No connection.')
    
    def reset_connection(self):
        '''Cleanly dispose of the current :attr:`connection`. Used
by client consumers only.'''
        if self.connected:
            conn = self._connection
            clone = copy(self)
            conn._current_consumer = clone
            self._connection = None
            clone.finished()
            return clone
        
    def finished(self, result=NOTHING):
        '''Call this method when done with this :class:`ProtocolConsumer`.
By default it calls the :meth:`Connection.finished` method of the
:attr:`connection` attribute.'''
        if self._connection:
            return self._connection.finished(self, result)
        else:
            # No connection available. Just fire the finish callback
            self.fire_event('finish', result)
            return result
        
    def connection_lost(self, exc):
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
        
        
class Connection(Protocol, TransportProxy):
    '''A client or server connection with an end-point. This is not
connected until :meth:`Protocol.connection_made` is called by the
:class:`Transport`. This class is the bridge between the :class:`Transport`
and the :class:`ProtocolConsumer`. It has a :class:`Protocol`
interface and it routes data arriving from the :attr:`transport` to
the :attr:`current_consumer`, an instance of :class:`ProtocolConsumer`.

It has two :ref:`one time events <one-time-event>`:

* *connection_made*
* *connection_lost*

and three :ref:`many times events <many-times-event>`:

* *pre_request*
* *data_received*
* *post_request*

.. attribute:: producer

    The producer of this :class:`Connection`, It is either a :class:`Server`
    or a client :class:`Client`.
    
.. attribute:: transport

    The :class:`Transport` of this protocol connection. Initialised once the
    :meth:`Protocol.connection_made` is called.
    
.. attribute:: consumer_factory

    A factory of :class:`ProtocolConsumer` instances for this
    :class:`Connection`.
    
.. attribute:: session

    Connection session number. Created by the :attr:`producer`.
    
.. attribute:: processed

    Number of separate :class:`ProtocolConsumer` processed by this connection.
    
.. attribute:: current_consumer

    The :class:`ProtocolConsumer` currently handling incoming data.
'''
    ONE_TIME_EVENTS = ('connection_made', 'connection_lost')
    MANY_TIMES_EVENTS = ('data_received', 'pre_request', 'post_request')
    #
    def __init__(self, address, session, timeout, consumer_factory, producer):
        super(Connection, self).__init__()
        self._address = address
        self._session = session 
        self._processed = 0
        self._timeout = timeout
        self._idle_timeout = None
        self._current_consumer = None
        self._consumer_factory = consumer_factory
        self._producer = producer
        
    def __repr__(self):
        return '%s session %s' % (nice_address(self._address), self._session)
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def session(self):
        return self._session
    
    @property
    def consumer_factory(self):
        return self._consumer_factory
    
    @property
    def current_consumer(self):
        return self._current_consumer
        
    @property
    def processed(self):
        return self._processed
    
    @property
    def address(self):
        return self._address
    
    @property
    def timeout(self):
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
        # Implements protocol connection_made
        self._transport = transport
        # let everyone know we have a connection with endpoint
        self.fire_event('connection_made')
        self._add_idle_timeout()
        
    def data_received(self, data):
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
        '''Implements the :meth:`Protocol.connection_lost` method. It performs
these actions in the following order:

* Cancel the idle timeout if set.
* Fire the *connection_lost* :ref:`one time event <one-time-event>` with *exc*
  as event data.
* Invokes the connection_lost method in the :attr:`current_consumer` if
  available.'''
        if self.fire_event('connection_lost', exc):
            self._cancel_timeout()
            if self._current_consumer:
                self._current_consumer.connection_lost(exc)
                             
    def upgrade(self, consumer_factory):
        '''Update the :attr:`consumer_factory` attribute with a new
:class:`ProtocolConsumer` factory. This function can be used when the protocol
specification changes during a response (an example is a WebSocket
response).'''
        self._consumer_factory = consumer_factory
        
    def finished(self, consumer, result=NOTHING):
        '''Call this method to finish with the the current *consumer*.
the *consumer* must be the same as the :attr:`current_consumer` attribute.'''
        if consumer and consumer is self._current_consumer:
            # make sure the current consumer is set to None before callbacks
            self._current_consumer = None
            self.fire_event('post_request', consumer, sender=self)
            consumer.fire_event('finish', result, sender=self)
            consumer._connection = None
        else:
            raise RuntimeError()
    
    ############################################################################
    ##    INTERNALS
    def _timed_out(self):
        LOGGER.info('%s idle for %d seconds. Closing connection.',
                        self, self._timeout)
        self.close()
         
    def _add_idle_timeout(self):
        if not self.closed and not self._idle_timeout and self._timeout:
            self._idle_timeout = self.event_loop.call_later(self._timeout,
                                                            self._timed_out)
            
    def _cancel_timeout(self):
        if self._idle_timeout:
            self._idle_timeout.cancel()
            self._idle_timeout = None
         
         
class Producer(Protocol):
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
    
    def new_connection(self, address, consumer_factory, producer=None):
        '''Called when a new :class:`Connection` is created. The *producer*
is either a :class:`Server` or a :class:`Client`. If the number of
:attr:`concurrent_connections` is greater or equal :attr:`max_connections`
a :class:`RuntimeError` is raised.'''
        if self._max_connections and self._received >= self._max_connections:
            raise RuntimeError('Too many connections')
        # increased the connections counter
        self._received = session = self._received + 1
        # new connection - not yet connected!
        producer = producer or self
        conn = self.connection_factory(address, session, self.timeout,
                                       consumer_factory, producer)
        # When the connection is made, add it to the set of
        # concurrent connections
        conn.bind_event('connection_made', self._add_connection)
        conn.copy_many_times_events(producer)
        conn.bind_event('connection_lost', partial(self._remove_connection,
                                                   conn))
        return conn
    
    def close_connections(self, connection=None, async=True):
        '''Close *connection* if specified, otherwise close all
active connections.'''
        if connection:
            connection.transport.close(async)
        else:
            for connection in list(self._concurrent_connections):
                connection.transport.close(async)
                
    def can_reuse_connection(self, connection, response):
        return True
            
    def _add_connection(self, connection):
        self._concurrent_connections.add(connection)
        
    def _remove_connection(self, connection, exc=None):
        # Called when the connection is lost
        self._concurrent_connections.discard(connection)
    