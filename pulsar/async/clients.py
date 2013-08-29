import socket
import math
from functools import partial, reduce
from threading import Lock

from pulsar.utils.pep import get_event_loop, new_event_loop, itervalues, range
from pulsar.utils.internet import is_socket_closed
from .defer import is_failure, multi_async

from .protocols import EventHandler, Producer

__all__ = ['ConnectionPool', 'Client', 'Request', 'SingleClient']

    
class Request(object):
    '''A :class:`Client` request.
    
A request object is hashable an it is used to select
the appropriate :class:`ConnectionPool` for the client request.

.. attribute:: address

    The socket address of the remote server
    
'''
    def __init__(self, address, timeout=0):
        self.address = address
        self.timeout = timeout
        
    @property
    def key(self):
        '''Attribute used for selecting the appropriate
:class:`ConnectionPool`'''
        return (self.address, self.timeout)
    
    def encode(self):
        raise NotImplementedError
    
    def create_connection(self, event_loop, connection):
        '''Called by a :class:`Client` when a new connection with
remote server is needed.'''
        res = event_loop.create_connection(lambda: connection,
                                           self.address[0],
                                           self.address[1])
        return res.add_callback(self._connection_made)
        
    def _connection_made(self, transport_protocol):
        _, connection = transport_protocol
        yield connection.event('connection_made')
        connection.current_consumer.new_request(self)
    
    
class ConnectionPool(Producer):
    '''A :class:`Producer` of of active connections for client
protocols. It maintains a live set of connections.

.. attribute:: address

    Address to connect to
    '''    
    def __init__(self, request, **params):
        params['timeout'] = request.timeout
        self.lock = Lock()
        super(ConnectionPool, self).__init__(**params)
        self._address = request.address
        self._available_connections = set()
    
    def __repr__(self):
        return repr(self.address)
    
    def __str__(self):
        return self.__repr__()
        
    @property
    def address(self):
        return self._address
    
    @property
    def available_connections(self):
        return len(self._available_connections)
        
    def release_connection(self, connection, response=None):
        '''Releases the *connection* back to the pool. This function remove
the *connection* from the set of concurrent connections and add it to the set
of available connections.

:parameter connection: The connection to release
:parameter response: Optional :class:`ProtocolConsumer` which consumed the
    connection.
'''
        with self.lock:
            self._concurrent_connections.discard(connection)
            if connection.producer.can_reuse_connection(connection, response):
                self._available_connections.add(connection)
        
    def get_or_create_connection(self, client):
        "Get or create a new connection for *client*"
        stale_connections = []
        with self.lock:
            try:
                closed = True
                while closed:
                    connection = self._available_connections.pop()
                    sock = connection.sock
                    closed = is_socket_closed(sock)
                    if closed and sock:
                        stale_connections.append(connection)
            except KeyError:
                connection = None
            else:
                # we have a connection, lets added it to the concurrent set
                self._concurrent_connections.add(connection)
        for sc in stale_connections:
            sc.transport.close()
        if connection is None:
            # build the new connection
            connection = self.new_connection(client.consumer_factory,
                                             producer=client)
            # Bind the post request event to the release connection function
            connection.bind_event('post_request', self._release_response)
            # Bind the connection_lost to connection to handle dangling connections
            connection.bind_event('connection_lost',
                                  partial(self._try_reconnect, connection))
        return connection
        
    ############################################################################
    ##    INTERNALS
    def _try_reconnect(self, connection, exc):
        # handle Read Exception on the transport
        if is_failure(exc, socket.timeout, socket.error):
            # Have we been here before?
            consumer = connection.current_consumer
            if consumer is None:
                # No consumer, The address was probably wrong. Connection Refused
                return
            client = connection.producer
            # The connection has processed request before and the consumer
            # has never received data. If the client allows it, try to
            # reconnect, it was probably a stale connection.
            retries = consumer.can_reconnect(client.max_reconnect, exc)
            if retries:
                connection._current_consumer = None
                lag = retries - 1
                if lag:
                    lag = client.reconnect_time_lag(lag)
                    connection.logger.debug('Reconnecting in %s seconds', lag)
                    loop = get_event_loop()
                    loop.call_later(lag, self._reconnect, client, consumer)
                else:
                    connection.logger.debug('Reconnecting')
                    self._reconnect(client, consumer)
                    
    def _reconnect(self, client, consumer):
        # get a new connection
        conn = self.get_or_create_connection(client)
        # Start the response without firing the events
        conn.set_consumer(consumer)
        consumer.new_request(consumer.current_request)
                
    def _remove_connection(self, connection, exc=None):
        with self.lock:
            super(ConnectionPool, self)._remove_connection(connection, exc)
            self._available_connections.discard(connection)
    
    def _release_response(self, response):
        #proxy to release_connection
        if getattr(response, 'release_connection', True):
            self.release_connection(response.connection, response)


class Client(EventHandler):
    '''A client for a remote server which handles one or more
:class:`ConnectionPool` of asynchronous connections.
It has the ``finish`` :ref:`one time event <one-time-event>` fired when calling
the :meth:`close` method.

in the same way as the :class:`Server` class, :class:`Client` has four
:ref:`many time events <many-times-event>`:

* ``connection_made`` a new connection is made.
* ``pre_request``, can be used to add information to the request
  to send to the remote server.
* ``post_request``, fired when a full response has been received. It can be
  used to post-process responses.
* ``connection_lost`` a connection dropped.

Most initialisation parameters have sensible defaults and don't need to be
passed for most use-cases. Additionally, they can also be set as class
attributes to override defaults.

:param max_connections: set the :attr:`max_connections` attribute.
:param timeout: set the :attr:`timeout` attribute.
:param force_sync: set the :attr:`force_sync` attribute.
:param event_loop: optional :class:`EventLoop` which set the :attr:`event_loop`.
:param connection_pool: optional factory which set the :attr:`connection_pool`.
    The :attr:`connection_pool` can also be set at class level.
:parameter client_version: optional version string for this :class:`Client`.

.. attribute:: event_loop

    The :class:`EventLoop` for this :class:`Client`. Can be ``None``.
    The preferred way to obtain the event loop is via the :meth:`get_event_loop`
    method rather than accessing this attribute directly.
    
.. attribute:: force_sync

    Force a :ref:`synchronous client <tutorials-synchronous>`, that is a
    client which has it own :class:`EventLoop` and blocks until a response
    is available.
    
    Default: `False`
'''
    max_reconnect = 1
    '''Can reconnect on socket error.'''
    connection_pools = None
    '''Dictionar of :class:`ConnectionPool`. If initialized at class level it
will remain as a class attribute, otherwise it will be an instance attribute.'''
    connection_pool = None
    '''Factory of :class:`ConnectionPool`.'''
    consumer_factory = None
    '''A factory of :class:`ProtocolConsumer` for sending and consuming data.'''
    connection_factory = None
    '''A factory of :class:`Connection`.'''
    client_version = ''
    '''An optional version for this client'''
    timeout = 0
    '''Optional timeout in seconds for idle connections. This is not the timeout
    for the sockets (which is always 0, i.e. asynchronous).'''
    max_connections = 0
    '''Maximum number of :attr:`concurrent_connections` allowed. Exceeding this
    number will result in a :class:`pulsar.utils.exceptions.TooManyConnections`
    error. ``0`` means an unlimited number is allowed.'''
    reconnecting_gap = 2
    '''Reconnecting gap in seconds.'''
    
    
    ONE_TIME_EVENTS = ('finish',)
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request','post_request',
                         'connection_lost')
    
    def __init__(self, max_connections=None, timeout=None, client_version=None,
                 trust_env=True, consumer_factory=None, max_reconnect=None,
                 force_sync=False, event_loop=None, connection_pool=None,
                 **params):
        super(Client, self).__init__()
        self.lock = Lock()
        self._closed = False
        self.trust_env = trust_env
        self.client_version = client_version or self.client_version
        self.timeout = timeout if timeout is not None else self.timeout
        self.connection_pool = (connection_pool or self.connection_pool or
                                ConnectionPool)
        if consumer_factory:
            self.consumer_factory = consumer_factory
        self.max_connections = max_connections or self.max_connections or 2**31
        if self.connection_pools is None:
            self.connection_pools = {}
        if max_reconnect:
            self.max_reconnect = max_reconnect
        self.force_sync = force_sync
        self.event_loop = event_loop
        self.setup(**params)
    
    def __str__(self):
        return self.__repr__()
    
    def __repr__(self):
        return self.__class__.__name__
    
    @property
    def concurrent_connections(self):
        '''Total number of concurrent connections.'''
        return reduce(lambda x,y: x + y, (p.concurrent_connections for p in\
                                          itervalues(self.connection_pools)), 0)
    
    @property
    def available_connections(self):
        '''Total number of available connections.'''
        return reduce(lambda x,y: x + y, (p.available_connections for p in\
                                          itervalues(self.connection_pools)), 0)
        
    @property
    def closed(self):
        '''``True`` if the :meth:`close` was invoked on this :class:`Client`.
A closed :class:`Client` cannot send :meth:`request` to remote servers.'''
        return self._closed
        
    def setup(self, **params):
        '''Setup the client.

Invoked at the end of initialisation with the additional parameters passed.
By default it does nothing.'''
        pass
    
    def get_event_loop(self):
        '''Return the :class:`EventLoop` used by this :class:`Client`.
The event loop can be set during initialisation. If :attr:`force_sync`
is ``True`` a specialised event loop is created.'''
        if self.event_loop:
            return self.event_loop
        elif self.force_sync:
            self.event_loop = new_event_loop(iothreadloop=False)
            return self.event_loop
        else:
            return get_event_loop()
    
    def hash(self, address, timeout, request):
        return hash((address, timeout))
    
    def request(self, *args, **params):
        '''Abstract method for creating a :class:`Request` to send to a
remote server. This method **must be implemented by subclasses** and should
return a :class:`ProtocolConsumer` via invoking the :meth:`response` method::

    def request(self, ...):
        ...
        request = ...
        return self.response(request)
    
'''
        raise NotImplementedError
    
    def response(self, request, response=None, new_connection=True):
        '''Once a ``request`` object has been constructed, the :meth:`request`
method can invoke this method to build the :class:`ProtocolConsumer` and
start the response. There should not be any reason to override this method.
This method is run on this client event loop (obtained via the
:meth:`get_event_loop` method) thread.

:parameter request: A custom :class:`Request` for the :class:`Client`.
:parameter response: A :class:`ProtocolConsumer` to reuse, otherwise
    ``None`` (Default).
:parameter new_connection: ``True`` if a new connection is required via
    the :meth:`get_connection` method. Default ``True``.
:rtype: An :class:`ProtocolConsumer` obtained form
    :attr:`consumer_factory`.
'''
        event_loop = self.get_event_loop()
        if response is None:
            response = self.consumer_factory()
        event_loop.call_now_threadsafe(self._response, event_loop,
                                       response, request, new_connection)
        if self.force_sync: # synchronous response
            event_loop.run_until_complete(response.on_finished)
        return response
    
    def get_connection(self, request):
        '''Get a suitable :class:`Connection` for ``request`` by first checking
if an available open connection can be used. Alternatively it creates
a new connection. This method invoks the
:meth:`ConnectionPool.get_or_create_connection` on the appropiate
connection pool.'''
        with self.lock:
            pool = self.connection_pools.get(request.key)
            if pool is None:
                pool = self.connection_pool(
                                request,
                                max_connections=self.max_connections,
                                connection_factory=self.connection_factory)
                self.connection_pools[request.key] = pool
        return pool.get_or_create_connection(self)
        
    def update_parameters(self, parameter_list, params):
        '''Update *param* with attributes of this :class:`Client` defined
in :attr:`request_parameters` tuple.'''
        for name in parameter_list:
            if name not in params:
                params[name] = getattr(self, name)
        return params
        
    def close_connections(self, async=True):
        '''Close all connections in each :attr:`connection_pools`.
        
:parameter async: if ``True`` flush the write buffer before closing (same
    as :class:`SocketTransport.close` method).
:return: a :class:`Deferred` called back once all connections are closed.'''
        all = []
        for p in self.connection_pools.values():
            all.append(p.close_connections(async=async))
        return multi_async(all)
            
    def close(self, async=True, timeout=5):
        '''Close all connections and fire the ``finish``
:ref:`one time event <one-time-event>`. Return the :class:`Deferred`
fired by the ``finish`` event.'''
        if not self.closed:
            self._closed = True
            event = self.close_connections(async)
            event.add_callback(lambda r: self.fire_event('finish'),
                               lambda f: self.fire_event('finish', f))
            event.set_timeout(timeout, self.get_event_loop())
        return self.event('finish')
        
    def abort(self):
        ''':meth:`close` all connections without waiting for active connections
        to finish.'''
        self.close(async=False)

    def can_reuse_connection(self, connection, response):
        '''Invoked by the :meth:`ConnectionPool.release_connection`, it checks
whether the *connection* can be reused in the future or it must be disposed.

:param connection: the :class:`Connection` to check.
:param response: the :class:`ProtocolConsumer` which last consumed the incoming
    data from the connection (it can be ``None``).
:return: ``True`` or ``False``.
'''
        return True
    
    def reconnect_time_lag(self, lag):
        lag = self.reconnect_time_lag*(math.log(lag) + 1)
        return round(lag, 1)
    
    def remove_pool(self, pool):
        key = None
        for key, p in self.connection_pools.items():
            if pool is p:
                break
        if key:
            self.connection_pools.pop(key)
            
    def upgrade(self, connection, protocol_factory, result=None):
        '''Upgrade an existing connection with a new protocol factory.
Return the upgraded connection only if the :attr:`Connection.current_consumer`
is available.'''
        protocol = connection.current_consumer
        if protocol:
            protocol.release_connection = False
            protocol.finished(result)
            connection.upgrade(protocol_factory)
            return connection
    
    def timeit(self, times, *args, **kwargs):
        '''Send ``times`` requests asynchronously and evaluate the time
taken to obtain all responses. In the standard implementation
this method will open ``times`` :class:`Connection` with the remote server.
Usage::

    client = Client(...)
    multi = client.timeit(100, ...)
    response = yield multi
    multi.total_time
    
:return: a :class:`MultiDeferred` which results in the list of results
  for the individual requests. Its :attr:`MultiDeferred.total_time` attribute
  indicates the number of seconds taken (once the deferred has been
  called back).
'''
        results = []
        for _ in range(times):
            r = self.request(*args, **kwargs)
            if hasattr(r, 'on_finished'):
                r = r.on_finished
            results.append(r)
        return multi_async(results)
    
    def _response(self, event_loop, response, request, new_connection,
                  result=None):
        # Actually execute the request. This method is always called on the
        # event loop thread
        try:
            conn = response.connection
            if new_connection or conn is None:
                # Get the connection for this request
                conn = self.get_connection(request)
                conn.set_consumer(response)
            if conn.transport is None:
                # There is no transport, we need to connect with server first
                return request.create_connection(
                    event_loop, conn).add_errback(response.finished)
            else:
                response.new_request(request)
        except Exception as e:
            response.finished(e)
        

class SingleClient(Client):
    '''A :class:`Client` which handle one connection only.'''
    def __init__(self, address, **kwargs):
        super(SingleClient, self).__init__(**kwargs)
        self.address = address
        self._consumer = None
    
    def response(self, request):
        resp = super(SingleClient, self).response
        self._consumer = resp(request, self._consumer, False)
        return self._consumer