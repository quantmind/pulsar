from pulsar import ProtocolError
from pulsar.async.access import NOTHING
from pulsar.async.defer import EventHandler

from .transport import TransportProxy, LOGGER


__all__ = ['ProtocolConsumer', 'Connection', 'Producer']


class ProtocolConsumer(EventHandler):
    '''The :class:`Protocol` consumer is one most important classes
in :ref:`pulsar framework <pulsar_framework>`. It is responsible for receiving
incoming data from a the :meth:`Protocol.data_received` method, decoding,
and producing responses, i.e. writing back to the client or server via
the :attr:`transport` attribute.

.. attribute:: connection

    The :class:`Connection` of this consumer
    
.. attribute:: protocol

    The :class:`Protocol` of this consumer
    
.. attribute:: transport

    The :class:`Transport` of this consumer
    
.. attribute:: on_finished

    A :class:`Deferred` called once the :class:`ProtocolConsumer` has
    finished consuming the :attr:`protocol`. It is called by the
    :attr:`connection` before disposing of this consumer.
'''
    ONE_TIME_EVENTS = ('start', 'finish')
    def __init__(self, connection):
        super(ProtocolConsumer, self).__init__()
        self._connection = connection
            
    @property
    def connection(self):
        return self._connection
    
    @property
    def event_loop(self):
        return self._connection.event_loop
    
    @property
    def sock(self):
        return self._connection.sock
        
    @property
    def transport(self):
        return self._connection.transport
    
    @property
    def address(self):
        return self._connection.address
    
    def on_connect(self):
        pass
    
    def begin(self):
        raise NotImplementedError
        
    def data_received(self, data):
        '''Feed new data into this :class:`ProtocolConsumer`. This method
must be implemented by subclasses.'''
        raise NotImplementedError
    
    def finished(self, result=NOTHING):
        '''Call this method when done with this :class:`ProtocolConsumer`.
By default it calls the :meth:`Connection.finished` method of the
:attr:`connection` attribute.'''
        return self._connection.finished(self, result)
    
    ############################################################################
    ###    TRANSPORT SHURTCUTS
    def write(self, data):
        '''Proxy of :meth:`Transport.write` method of :attr:`transport`.'''
        self.transport.write(data)
            
    def writelines(self, lines):
        '''Proxy of :meth:`Transport.writelines` method of :attr:`transport`.'''
        self.transport.writelines(lines)
        
        
class Connection(TransportProxy):
    '''A client or server connection. It contains the :class:`Protocol`, the
transport producer (:class:`Server` or :class:`Client`), a session
number and a factory of :class:`ProtocolConsumer`.

.. attribute:: transport

    The :class:`Transport` of this protocol connection
    
.. attribute:: producer

    The producer of this :class:`Connection`, It is either a :class:`Server`
    or a client :class:`Client`.
    
.. attribute:: consumer_factory

    A factory of :class:`ProtocolConsumer` instances for this :class:`Protocol`
    
.. attribute:: session

    Connection session number. Created by the :attr:`producer`
    
.. attribute:: processed

    Number of separate requests processed by this connection.
    
.. attribute:: current_consumer

    The :class:`ProtocolConsumer` currently handling incoming data.
'''
    def __init__(self, address, producer, session, consumer_factory):
        self._address = address
        self._producer = producer
        self._session = session 
        self._processed = 0
        self._current_consumer = None
        self._consumer_factory = consumer_factory
        self._time_out = False
        
    def __repr__(self):
        address = self._address
        if isinstance(address, tuple):
            address = ':'.join((str(s) for s in address[:2]))
        return '%s session %s' % (address, self._session)
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def producer(self):
        return self._producer
    
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
    
    def set_consumer(self, consumer):
        assert self._current_consumer is None, 'Consumer is not None'
        self._current_consumer = consumer
        self._producer.fire_event('pre_request', consumer)
        self._processed += 1
    
    def connection_made(self, transport):
        self._transport = transport
        self._producer._add_connection(self)
        transport.bind_event('data_received', self.data_received)
        transport.bind_event('connection_lost', self.connection_lost)
        transport.bind_event('timeout', self._timed_out)
        transport.add_read_write()
        
    def data_received(self, data):
        while data:
            consumer = self._current_consumer
            if consumer is None:
                # New consumer
                consumer = self._consumer_factory(self)
                self.set_consumer(consumer) 
            data = consumer.data_received(data)
            if data and self._current_consumer:
                # if data is returned from the response feed method and the
                # response has not done yet raise a Protocol Error
                raise ProtocolError
    
    def connection_lost(self, exc):
        self.producer._remove_connection(self)
        if self._time_out:
            LOGGER.info('%s idle for %d seconds. Closed connection.',
                        self, self._time_out)
                             
    def upgrade(self, consumer_factory):
        '''Update the :attr:`consumer_factory` attribute with a new
:class:`ProtocolConsumer` factory. This function can be used when the protocol
specification changes during a response (an example is a WebSocket
response).'''
        self._consumer_factory = consumer_factory
        
    def finished(self, response, result=NOTHING):
        '''Call this method with the current response to close the current
consumer.'''
        if response is self._current_consumer:
            self._producer.fire_event('post_request', response)
            response.fire_event('finish', result)
            self._current_consumer = None
            response._connection = None
        else:
            raise RuntimeError()
    
    def _timed_out(self, timeout):
         self._time_out = timeout
         
         
class Producer(EventHandler):
    '''A Producer of :class:`Connection` with remote servers or clients.
It is the base class for both :class:`Server` and :class:`ConnectionPool`.
The main method in this class is :meth:`new_connection` where a new
:class:`Connection` is created and added to the set of
:attr:`concurrent_connections`.

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
    def __init__(self, max_connections=0, connection_factory=None):
        super(Producer, self).__init__()
        self._received = 0
        self._max_connections = max_connections
        self._concurrent_connections = set()
        if connection_factory:
            self.connection_factory = connection_factory
    
    @property
    def timeout(self):
        raise NotImplemented
    
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
        ''''Called when a new connection is created'''
        if self._max_connections and self._received >= self._max_connections:
            raise RuntimeError('Too many connections')
        self._received = self._received + 1
        producer = producer or self 
        return self.connection_factory(address, producer,
                                       self._received, consumer_factory)
    
    def close_connections(self, connection=None):
        '''Close *connection* if specified, otherwise close all
active connections.'''
        if connection:
            connection.transport.close()
        else:
            for connection in self._concurrent_connections:
                connection.transport.close()
            
    def _add_connection(self, conn):
        self._concurrent_connections.add(conn)
        
    def _remove_connection(self, connection):
        self._concurrent_connections.discard(connection)
        
    def close(self):
        raise NotImplementedError