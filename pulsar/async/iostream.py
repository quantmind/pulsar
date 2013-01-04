import sys
import io
import time
import logging
import socket
import errno
from collections import deque
from threading import current_thread, Thread

from pulsar.utils.system import IObase
from pulsar import create_socket, server_socket, create_client_socket,\
                     wrap_socket, defaults, create_connection, CouldNotParse,\
                     get_socket_timeout, Timeout, BaseSocket, Synchronized
from pulsar.utils.httpurl import IOClientRead
from pulsar.utils.structures import merge_prefix
from .defer import Deferred, is_async, is_failure, async, maybe_async,\
                        safe_async, log_failure, NOT_DONE, range
from .eventloop import IOLoop, loop_timeout
from .access import PulsarThread, thread_ioloop, get_actor

LOGGER = logging.getLogger('pulsar.iostream')


__all__ = ['AsyncIOStream',
           'ProtocolSocket',
           'ClientSocket',
           'Client',
           'AsyncConnection',
           'AsyncResponse',
           'AsyncSocketServer',
           'WRITE_BUFFER_MAX_SIZE']

WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb

ASYNC_ERRNO = (errno.EWOULDBLOCK, errno.EAGAIN, errno.EINPROGRESS)
def async_error(e):
    return e.args and e.args[0] in ASYNC_ERRNO
    

class AsyncIOStream(IObase, BaseSocket):
    ''':ref:`Framework class <pulsar_framework>` to write and read
from a non-blocking socket. It is used everywhere in :mod:`pulsar` for
handling asynchronous :meth:`write` and :meth:`read` operations with
`callbacks` which can be used to act when data has just been sent or has
just been received.

It was originally forked from tornado_ IOStream and subsequently
adapted to pulsar :ref:`concurrent framework <design>`.

.. attribute:: socket

    A :class:`Socket` which might be connected or unconnected.

.. attribute:: timeout

    A timeout in second which is used when waiting for a
    data to be available for reading. If timeout is a positive number,
    every time the :class:`AsyncIOStream` performs a :meth:`read`
    operation a timeout is also created on the :attr:`ioloop`.
'''
    _error = None
    _socket = None
    _state = None
    _read_timeout = None
    _read_callback = None
    _read_length = None
    _write_callback = None
    _close_callback = None
    _connect_callback = None

    def __init__(self, socket=None, max_buffer_size=None,
                 read_chunk_size=None, timeout=None):
        self.sock = socket
        self._read_callback_timeout = timeout
        self.max_buffer_size = max_buffer_size or 104857600
        self.read_chunk_size = read_chunk_size or io.DEFAULT_BUFFER_SIZE
        self._read_buffer = deque()
        self._write_buffer = deque()

    def __repr__(self):
        if self.sock:
            return '%s (%s)' % (self.sock, self.state_code)
        else:
            return '(closed)'

    def __str__(self):
        return self.__repr__()

    #######################################################    STATES
    @property
    def connecting(self):
        return self._connect_callback is not None

    @property
    def reading(self):
        """Returns true if we are currently reading from the stream."""
        return self._read_callback is not None

    @property
    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return bool(self._write_buffer)

    @property
    def closed(self):
        '''Boolean indicating if the :attr:`sock` is closed.'''
        return self.sock is None

    @property
    def state(self):
        return self._state
    
    @property
    def error(self):
        return self._error

    @property
    def state_code(self):
        s = []
        if self.closed:
            return 'closed'
        if self.connecting:
            s.append('connecting')
        if self.writing:
            s.append('writing')
        if self.reading:
            s.append('reading')
        return ' '.join(s) if s else 'idle'

    @property
    def ioloop(self):
        return thread_ioloop()
    
    def settimeout(self, value):
        pass

    def _set_socket(self, sock):
        if self._socket is None:
            self._socket = wrap_socket(sock)
            self._state = None
            if self._socket is not None:
                self._socket.settimeout(0)
        else:
            raise RuntimeError('Cannot set socket. Close the existing one.')
    def _get_socket(self):
        return self._socket
    sock = property(_get_socket, _set_socket)

    #######################################################    ACTIONS
    def connect(self, address):
        """Connects the socket to a remote address without blocking.
May only be called if the socket passed to the constructor was not available
or it was not previously connected.  The address parameter is in the
same format as for socket.connect, i.e. a (host, port) tuple or a string
for unix sockets.
If callback is specified, it will be called when the connection is completed.
Note that it is safe to call IOStream.write while the
connection is pending, in which case the data will be written
as soon as the connection is ready.  Calling IOStream read
methods before the socket is connected works on some platforms
but is non-portable."""
        if self._state is None and not self.connecting:
            if self.sock is None:
                self.sock = create_client_socket(address)
            try:
                self.sock.connect(address)
            except socket.error as e:
                # In non-blocking mode connect() always raises an exception
                if not async_error(e):
                    LOGGER.warning('Connect error on %s: %s', self, e)
                    self.close()
                    return
            callback = Deferred(description='%s connect callback' % self)
            self._connect_callback = callback
            self._add_io_state(self.WRITE)
            return callback
        else:
            raise RuntimeError('Cannot connect. State is %s.' % self.state_code)

    def read(self, length=None):
        """Starts reading data from the :attr:`sock`. It returns a
:class:`Deferred` which will be called back once data is available.
If this function is called while this class:`AsyncIOStream` is already reading
a RuntimeError occurs.

:rtype: a :class:`pulsar.Deferred` instance.

One common pattern of usage::

    def parse(data):
        ...

    io = AsyncIOStream(socket=sock)
    io.read().add_callback(parse)

"""
        if self.reading:
            raise RuntimeError("Asynchronous stream %s already reading!" %
                               str(self.address))
        if self.closed:
            return self._get_buffer(self._read_buffer)
        else:
            callback = Deferred(description='%s read callback' % self)
            self._read_callback = callback
            self._read_length = length
            self._add_io_state(self.READ)
            if self._read_timeout:
                try:
                    self.ioloop.remove_timeout(self._read_timeout)
                except ValueError:
                    pass
            self._read_timeout = loop_timeout(callback,
                                              self._read_callback_timeout,
                                              self.ioloop)
            return callback
    recv = read

    def write(self, data):
        """Write the given *data* to this stream. If there was previously
buffered write data and an old write callback, that callback is simply
overwritten with this new callback.

:rtype: a :class:`Deferred` instance or the number of bytes written.
        """
        self._check_closed()
        if data:
            assert isinstance(data, bytes)
            if len(data) > WRITE_BUFFER_MAX_SIZE:
                for i in range(0, len(data), WRITE_BUFFER_MAX_SIZE):
                    self._write_buffer.append(data[i:i+WRITE_BUFFER_MAX_SIZE])
            else:
                self._write_buffer.append(data)
        #
        if not self.connecting:
            tot_bytes = self._handle_write()
            # data still in the buffer
            if self._write_buffer:
                callback = Deferred(description='%s write callback' % self)
                self._write_callback = callback
                self._add_io_state(self.WRITE)
                return callback
            else:
                return tot_bytes
    sendall = write

    def close(self):
        """Close the :attr:`sock` and call the *callback* if it was
setup using the :meth:`set_close_callback` method."""
        if not self.closed:
            exc_info = sys.exc_info()
            if any(exc_info):
                self._error = exc_info[1]
            if self._state is not None:
                self.ioloop.remove_handler(self.fileno())
            self.sock.close()
            self._socket = None
            if self._close_callback:
                self._may_run_callback(self._close_callback)

    def set_close_callback(self, callback):
        """Call the given callback when the stream is closed."""
        self._close_callback = callback

    #######################################################    INTERNALS
    def read_to_buffer(self):
        #Reads from the socket and appends the result to the read buffer.
        #Returns the number of bytes read.
        length = self._read_length or self.read_chunk_size
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # SSL sockets do some internal buffering, and if the data is
        # sitting in the SSL object's buffer select() and friends
        # can't see it; the only way to find out if it's there is to
        # try to read it.
        while True:
            try:
                chunk = self.sock.recv(length)
            except socket.error as e:
                if async_error(e):
                    chunk = None
                else:
                    raise
            if not chunk:
                # if chunk is b'' close the socket
                if chunk is not None:
                    self.close()
                break
            self._read_buffer.append(chunk)
            if self._read_buffer_size() >= self.max_buffer_size:
                LOGGER.error("Reached maximum read buffer size")
                self.close()
                raise IOError("Reached maximum read buffer size")
            if len(chunk) < length:
                break
        return self._read_buffer_size()

    def _read_buffer_size(self):
        return sum(len(chunk) for chunk in self._read_buffer)

    def _may_run_callback(self, c, result=None):
        try:
            # Make sure that any uncaught error is logged
            log_failure(c.callback(result))
        except:
            # Close the socket on an uncaught exception from a user callback
            # (It would eventually get closed when the socket object is
            # gc'd, but we don't want to rely on gc happening before we
            # run out of file descriptors)
            self.close()

    def _handle_connect(self):
        callback = self._connect_callback
        self._connect_callback = None
        self._may_run_callback(callback)

    def _handle_read(self):
        try:
            try:
                result = self.read_to_buffer()
            except (socket.error, IOError, OSError) as e:
                if e.args and e.args[0] == errno.ECONNRESET:
                    # Treat ECONNRESET as a connection close rather than
                    # an error to minimize log spam  (the exception will
                    # be available on self.error for apps that care).
                    result = 0
                else:
                    raise
        except Exception:
            result = 0
            LOGGER.warning("Read error on %s.", self, exc_info=True)
        if result == 0:
            self.close()
        buffer = self._get_buffer(self._read_buffer)
        if self.reading:
            callback = self._read_callback
            self._read_callback = None
            self._may_run_callback(callback, buffer)

    def _handle_write(self):
        # keep count how many bytes we write
        tot_bytes = 0
        while self._write_buffer:
            try:
                sent = self.sock.send(self._write_buffer[0])
                if sent == 0:
                    # With OpenSSL, after send returns EWOULDBLOCK,
                    # the very same string object must be used on the
                    # next call to send.  Therefore we suppress
                    # merging the write buffer after an EWOULDBLOCK.
                    # A cleaner solution would be to set
                    # SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, but this is
                    # not yet accessible from python
                    # (http://bugs.python.org/issue8240)
                    break
                merge_prefix(self._write_buffer, sent)
                self._write_buffer.popleft()
                tot_bytes += sent
            except socket.error as e:
                if async_error(e):
                    break
                else:
                    LOGGER.warning("Write error on %s: %s", self, e)
                    self.close()
                    return
        if not self._write_buffer and self._write_callback:
            callback = self._write_callback
            self._write_callback = None
            self._may_run_callback(callback, tot_bytes)
        return tot_bytes

    def _check_closed(self):
        if not self.sock:
            raise IOError("Stream is closed")

    def _get_buffer(self, dq):
        buff = b''.join(dq)
        dq.clear()
        return buff

    def _handle_events(self, fd, events):
        # This is the actual callback from the event loop
        if not self.sock:
            LOGGER.warning("Got events for closed stream %d", fd)
            return
        try:
            if events & self.READ:
                self._handle_read()
            if not self.sock:
                return
            if events & self.WRITE:
                if self.connecting:
                    self._handle_connect()
                self._handle_write()
            if not self.sock:
                return
            if events & self.ERROR:
                # We may have queued up a user callback in _handle_read or
                # _handle_write, so don't close the IOStream until those
                # callbacks have had a chance to run.
                self.ioloop.add_callback(self.close)
                return
            state = self.ERROR
            if self.reading:
                state |= self.READ
            if self.writing:
                state |= self.WRITE
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.ioloop.update_handler(self.fileno(), self._state)
        except:
            self.close()
            raise

    def _add_io_state(self, state):
        if self.sock is None:
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            # If the state was not set add the handler to the event loop
            self._state = self.ERROR | state
            self.ioloop.add_handler(self, self._handle_events, self._state)
        elif not self._state & state:
            # update the handler
            self._state = self._state | state
            self.ioloop.update_handler(self, self._state)


class EchoParser:
    '''A simple echo protocol'''
    def encode(self, data):
        return data
    
    def decode(self, data):
        return bytes(data), bytearray()
    
    
class ProtocolSocket(BaseSocket):
    '''A :class:`BaseSocket` with a protocol for encoding and decoding
messages. This is the base class for :class:`AsyncSocketServer`,
:class:`AsyncConnection` and :class:`ClientSocketHandler`.

.. attribute:: on_closed

    A :class:`Deferred` which receives a callback once the
    :meth:`close` method is invoked.
 
'''
    protocol_factory = EchoParser
    '''A callable for building the protocol to encode and decode messages. This
    attribute can be specified as class attribute or via the constructor.
    A :ref:`protocol <socket-protocol>` must be an instance of a class
    exposing the ``encode`` and ``decode`` methods.'''
    _closing_socket = False
    def __new__(cls, *args, **kwargs):
        o = super(ProtocolSocket, cls).__new__(cls)
        o.on_closed = Deferred(
                        description='on_closed %s callback' % cls.__name__)
        o.time_started = time.time()
        o.time_last = o.time_started
        f = kwargs.get('protocol_factory')
        if f:
            o.protocol_factory = f
        o.received = 0
        return o

    @property
    def closed(self):
        '''True if the socket is closed.'''
        return self.sock.closed

    def on_close(self, failure=None):
        '''Callback just before closing the socket'''
        pass

    def close(self, msg=None):
        '''Close this socket and log the failure if there was one.'''
        if self._closing_socket:
            return msg
        self._closing_socket = True
        if is_failure(msg):
            if isinstance(msg.trace[1], Timeout):
                if not msg.logged:
                    msg.logged = True
                    LOGGER.info('Closing %s on timeout.', self)
            else:
                log_failure(msg)
        self.on_close(msg)
        self.sock.close()
        return self.on_closed.callback(msg)
    
    
class ClientSocketHandler(ProtocolSocket):
    '''A :class:`ProtocolSocket` for clients.
This class can be used with synchronous and asynchronous socket
for both a "client" socket and a server connection socket (the socket
obtained from a server socket via the ``connect`` function).

.. attribute:: protocol

    An object obtained by the :attr:`ProtocolSocket.protocol_factory` method.
    It must implement the ``encode`` and ``decode`` methods used, respectively,
    by clients and servers.
    
.. attribute:: buffer

    A bytearray containing data received from the socket. This buffer
    is not empty when more data is required to form a message.
    
.. attribute:: remote_address

    The remote address for the socket communicating with this
    :class:`ClientSocketHandler`.
'''
    def __init__(self, socket, address, timeout=None, read_timeout=None,
                 **kwargs):
        '''Create a client or client-connection socket.

:parameter socket: a client or client-connection socket
:parameter address: The address of the remote client/server
:parameter protocol_factory: A callable used for creating
    :attr:`ProtoSocket.protocol` instance.
:parameter timeout: A timeout in seconds for the socket. Same rules as
    the ``socket.settimeout`` method in the standard library. A value of 0
    indicates an asynchronous socket.
:parameter read_timeout: A timeout in seconds for asynchronous operations. This
    value is only used when *timeout* is 0 in the constructor
    of a :class:`AsyncIOStream`.
'''
        self._socket_timeout = get_socket_timeout(timeout)
        self._set_socket(socket, read_timeout)
        self.remote_address = address
        self.protocol = self.protocol_factory()
        self.buffer = bytearray()

    def __repr__(self):
        return str(self.remote_address)
    __str__ = __repr__

    def _set_socket(self, sock, read_timeout=None):
        if not isinstance(sock, AsyncIOStream):
            if self._socket_timeout == 0:
                sock = AsyncIOStream(sock)
            else:
                sock = wrap_socket(sock)
                sock.settimeout(self._socket_timeout)
        self.sock = sock
        if self.async:
            close_callback = Deferred().add_callback(self.close)
            self.sock.set_close_callback(close_callback)
            self.read_timeout = read_timeout
            
    def _get_read_timeout(self):
        if self.async:
            return self.sock._read_callback_timeout
        else:
            return self._socket_timeout
    def _set_read_timeout(self, value):
        if self.async:
            self.sock._read_callback_timeout = value
        else:
            self._socket_timeout = value
            self.sock.settimeout(self._socket_timeout)
    read_timeout = property(_get_read_timeout, _set_read_timeout) 


class ClientSocket(ClientSocketHandler, IOClientRead):
    '''Synchronous/Asynchronous client for a remote socket server. This client
maintain a connection with remote server and exchange data by writing and
reading from the same socket connection.'''
    def __init__(self, *args, **kwargs):
        super(ClientSocket, self).__init__(*args, **kwargs)
        self.exec_queue = deque()
        self.processing = False
        
    @classmethod
    def connect(cls, address, **kwargs):
        '''Create a new :class:`ClientSocket` connected at *address*.'''
        sock = create_connection(address)
        return cls(sock, address, **kwargs)

    def send(self, data):
        '''Send data to remote server'''
        self.time_last = time.time()
        data = self.protocol.encode(data)
        return self.sock.write(data)
        
    def execute(self, data):
        '''Send and read data from socket. It makes sure commands are queued
and executed in an orderly fashion. For asynchronous connection it returns
a :class:`Deferred` called once data has been received from the server and
parsed.'''
        if self.async:
            cbk = Deferred()
            if data:
                cbk.addBoth(self._got_result)
                self.exec_queue.append((data, cbk))
                self.sock.ioloop.add_callback(self._consume_next)
            else:
                cbk.callback(None)
            return cbk
        elif data:
            self.send(data)
            return self._read()
        
    def parsedata(self, data):
        '''We got some data to parse'''
        parsed_data = self._parsedata(data)
        if parsed_data:
            self.received += 1
            return parsed_data
        
    def _consume_next(self):
        # This function is always called in the socket IOloop
        if not self.processing and self.exec_queue:
            self.processing = True
            data, cbk = self.exec_queue.popleft()
            msg = safe_async(self.send, (data,))\
                    .add_callback(self._read, self.close)
            msg = maybe_async(msg)
            if is_async(msg):
                msg.addBoth(cbk.callback)
            else:
                cbk.callback(msg)
            
    def _got_result(self, result):
        # This callback is always executed in the socket IOloop
        self.processing = False
        # keep on consuming if needed
        self._consume_next()
        return result

    def _parsedata(self, data):
        buffer = self.buffer
        if data:
            # extend the buffer
            buffer.extend(data)
        elif not buffer:
            return
        try:
            parsed_data, buffer = self.protocol.decode(buffer)
        except CouldNotParse:
            LOGGER.warn('Could not parse data', exc_info=True)
            parsed_data = None
            buffer = bytearray()
        self.buffer = buffer
        return parsed_data


class Client(ClientSocket):

    def reconnect(self):
        if self.closed:
            sock = create_connection(self.remote_address, blocking=True)
            self._set_socket(sock)


class ReconnectingClient(Client):

    def send(self, data):
        self.reconnect()
        return super(ReconnectingClient, self).send(data)


class AsyncResponse(object):
    '''An asynchronous server response is created once an
:class:`AsyncConnection` has available parsed data from a read operation.
Instances of this class are iterable and produce chunk of data to send back
to the remote client.

The ``__iter__`` is the only method which **needs** to be implemented by
derived classes. If an empty byte is yielded, the asynchronous engine
will resume the iteration after one loop in the actor event loop.

.. attribute:: connection

    The :class:`AsyncConnection` for this response
    
.. attribute:: server

    The :class:`AsyncSocketServer` for this response

.. attribute:: parsed_data

    Parsed data from remote client
'''
    def __init__(self, connection, parsed_data):
        self.connection = connection
        self.parsed_data = parsed_data

    @property
    def server(self):
        return self.connection.server

    @property
    def protocol(self):
        return self.connection.protocol

    @property
    def sock(self):
        return self.connection.sock

    def __iter__(self):
        # by default it echos the client message
        yield self.protocol.encode(self.parsed_data)


class AsyncConnection(ClientSocketHandler):
    '''An asynchronous client connection for a :class:`AsyncSocketServer`.
The connection maintains the client socket open for as long as it is required.
A connection can handle several request/responses until it is closed.

.. attribute:: server

    The class :class:`AsyncSocketServer` which created this
    :class:`AsyncConnection`.

.. attribute:: response_class

    Class or callable for building an :class:`AsyncResponse` object. It is
    initialised by the :class:`AsyncSocketServer.response_class` but it can be
    changed at runtime when upgrading connections to new protocols. An example
    is the websocket protocol.
'''
    def __init__(self, sock, address, server, **kwargs):
        if not isinstance(sock, AsyncIOStream):
            sock = AsyncIOStream(sock, timeout=server.timeout)
        super(AsyncConnection, self).__init__(sock, address, **kwargs)
        self.server = server
        self.response_class = server.response_class
        server.connections.add(self)
        self.handle().add_errback(self.close)

    @async(max_errors=1, description='Asynchronous client connection generator')
    def handle(self):
        while not self.closed:
            # Read the socket
            outcome = self.sock.read()
            yield outcome
            # if we are here it means no errors occurred so far and
            # data is available to process (max_errors=1)
            self.time_last = time.time()
            buffer = self.buffer
            buffer.extend(outcome.result)
            parsed_data = True
            # IMPORTANT! Consume all data until the protocol returns nothing.
            while parsed_data:
                parsed_data = self.request_data()
                if parsed_data:
                    self.received += 1
                    # The response is an iterable (same as WSGI response)
                    for data in self.response_class(self, parsed_data):
                        if data:
                            yield self.write(data)
                        else: # The response is not ready. release the loop
                            yield NOT_DONE
    
    def request(self, response=None):
        if self._current_request is None:
            self._current_request = AsyncRequest(self)
        request = self._current_request
        if response is not None:
            self._current_request = None
            request.callback(response)
        return request

    def request_data(self):
        '''This function is called when data to parse is available on the
:attr:`ClientSocket.buffer`. It should return parsed data or ``None`` if
more data in the buffer is required.'''
        buffer = self.buffer
        if not buffer:
            return
        self.protocol.connection = self
        try:
            parsed_data, buffer = self.protocol.decode(buffer)
        except:
            LOGGER.error('Could not parse data', exc_info=True)
            raise
        self.buffer = buffer
        return parsed_data

    @property
    def actor(self):
        return self.server.actor

    def on_close(self, failure=None):
        self.server.connections.discard(self)

    def write(self, data):
        '''Write data to socket.'''
        return self.sock.write(data)


class AsyncSocketServer(ProtocolSocket):
    '''A :class:`ProtocolSocket` for asynchronous servers which listen
for requests on a socket.

.. attribute:: actor

    The :class:`Actor` running this :class:`AsyncSocketServer`.

.. attribute:: ioloop

    The :class:`IOLoop` used by this :class:`AsyncSocketServer` for
    asynchronously sending and receiving data.

.. attribute:: connections

    The set of all open :class:`AsyncConnection`

.. attribute:: onthread

    If ``True`` the server has its own :class:`IOLoop` running on a separate
    thread of execution. Otherwise it shares the :attr:`actor.requestloop`

.. attribute:: timeout

    The timeout when reading data in an asynchronous way.
    
'''
    thread = None
    _started = False
    connection_class = AsyncConnection
    '''A subclass of :class:`AsyncConnection`. A new instance of this class is
constructued each time a new connection has been established by the
:meth:`accept` method.'''
    response_class = AsyncResponse
    '''A subclass of :class:`AsyncResponse` for handling responses to clients
    once data has been received and processed.'''
    
    def __init__(self, actor, socket, onthread=False, connection_class=None,
                 response_class=None, timeout=None, **kwargs):
        self.actor = actor
        self.connection_class = connection_class or self.connection_class
        self.response_class = response_class or self.response_class
        self.sock = wrap_socket(socket)
        self.connections = set()
        self.timeout = timeout
        self.on_connection_callbacks = []
        if onthread:
            # Create a pulsar thread and starts it
            self.__ioloop = IOLoop()
            self.thread = PulsarThread(name=self.name, target=self._run)
            self.thread.actor = actor
            actor.requestloop.add_callback(lambda: self.thread.start())
        else:
            self.__ioloop = actor.requestloop
            actor.requestloop.add_callback(self._run)
        
    @classmethod
    def make(cls, actor=None, bind=None, backlog=None, **kwargs):
        if actor is None:
            actor = get_actor()
        if not backlog:
            if actor:
                backlog = actor.cfg.backlog
            else:
                backlog = defaults.BACKLOG
        if bind:
            socket = create_socket(bind, backlog=backlog)
        else:
            socket = server_socket(backlog=backlog)
        return cls(actor, socket, **kwargs)

    @property
    def onthread(self):
        return self.thread is not None
    
    @property
    def name(self):
        return '%s %s' % (self.__class__.__name__, self.address)

    def __repr__(self):
        return self.name
    __str__ = __repr__

    @property
    def ioloop(self):
        return self.__ioloop

    @property
    def active_connections(self):
        return len(self.connections)

    def quit_connections(self):
        for c in list(self.connections):
            c.close()
            
    def on_close(self, failure=None):
        self.quit_connections()
        self.ioloop.remove_handler(self)
        if self.onthread:
            self.__ioloop.stop()
            # We join the thread
            if current_thread() != self.thread:
                try:
                    self.thread.join()
                except RuntimeError:
                    pass

    ############################################################## INTERNALS
    def _run(self):
        # add new_connection READ handler to the eventloop and starts the
        # eventloop if it was not already started.
        self.actor.logger.debug('Registering %s with event loop.', self)
        self.ioloop.add_handler(self,
                                self._on_connection,
                                self.ioloop.READ)
        self.ioloop.start()

    def _on_connection(self, fd, events):
        '''Called when a new connection is available.'''
        # obtain the client connection
        for callback in self.on_connection_callbacks:
            c = callback(fd, events)
            if c is not None:
                return c
        return self.accept()

    def accept(self):
        '''Accept a new connection from a remote client'''
        client, client_address = self.sock.accept()
        if client:
            self.received += 1
            return self.connection_class(client, client_address, self,
                                         protocol_factory=self.protocol_factory)
