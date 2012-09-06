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
from .defer import Deferred, is_async, is_failure, async, maybe_async,\
                        safe_async, log_failure, NOT_DONE
from .eventloop import IOLoop, loop_timeout
from .access import PulsarThread, thread_ioloop, get_actor

iologger = logging.getLogger('pulsar.iostream')


__all__ = ['AsyncIOStream',
           'BaseSocketHandler',
           'ClientSocket',
           'Client',
           'AsyncConnection',
           'AsyncResponse',
           'AsyncSocketServer',
           'MAX_BODY']

MAX_BODY = 1024 * 128
    

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

.. attribute:: _read_callback_timeout

    A timeout in second which is used when waiting for a
    data to be available for reading. If timeout is a positive number,
    every time we the :class:`AsyncIOStream` perform a :meth:`read`
    operation a timeout is also created on the :attr:`ioloop`.
'''
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
        self._write_buffer_frozen = False
        self.log = iologger

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
                if e.args[0] not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                    raise
            callback = Deferred(description = '%s connect callback' % self)
            self._connect_callback = callback
            self._add_io_state(self.WRITE)
            return callback
        else:
            if self._state is not None:
                raise RuntimeError('Cannot connect. State is %s.'\
                                   .format(self.state_code))
            else:
                raise RuntimeError('Cannot connect while connecting.')

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

:rtype: a :class:`Deferred` instance.
        """
        if data:
            self._check_closed()
            self._write_buffer.append(data)
            tot_bytes = self._handle_write()
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
        if self.sock is not None:
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
        # Read what is available in the buffer
        while True:
            try:
                chunk = self.sock.recv(length)
            except socket.error as e:
                self.close()
                raise
            if chunk is None:
                break
            self._read_buffer.append(chunk)
            if self._read_buffer_size() >= self.max_buffer_size:
                self.log.error("Reached maximum read buffer size")
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
            # Read from the socket until we get EWOULDBLOCK or equivalent.
            # SSL sockets do some internal buffering, and if the data is
            # sitting in the SSL object's buffer select() and friends
            # can't see it; the only way to find out if it's there is to
            # try to read it.
            result = self.read_to_buffer()
        except Exception:
            result = 0
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
                if not self._write_buffer_frozen:
                    # On windows, socket.send blows up if given a
                    # write buffer that's too large, instead of just
                    # returning the number of bytes it was able to
                    # process.  Therefore we must not call socket.send
                    # with more than 128KB at a time.
                    buff = self._get_buffer(self._write_buffer, MAX_BODY)
                else:
                    buff = self._write_buffer.popleft() or b''
                sent = self.sock.send(buff)
                if sent == 0:
                    raise socket.error()
                tot_bytes += sent
            except socket.error as e:
                if e.args and e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    # With OpenSSL, after send returns EWOULDBLOCK,
                    # the very same string object must be used on the
                    # next call to send.  Therefore we suppress
                    # merging the write buffer after an EWOULDBLOCK.
                    # A cleaner solution would be to set
                    # SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, but this is
                    # not yet accessible from python
                    # (http://bugs.python.org/issue8240)
                    self._write_buffer_frozen = True
                    break
                else:
                    self.log.warning("Write error on %s.", self, exc_info=True)
                    self.close()
                    return tot_bytes
        return tot_bytes

        if not self._write_buffer and self._write_callback:
            callback = self._write_callback
            self._write_callback = None
            self._may_run_callback(callback, tot_bytes)

    def _check_closed(self):
        if not self.sock:
            raise IOError("Stream is closed")

    def _get_buffer(self, dq, size = None):
        if size is None:
            buff = b''.join(dq)
            dq.clear()
        else:
            remaining = size
            prefix = []
            while dq and remaining > 0:
                chunk = dq.popleft()
                if len(chunk) > remaining:
                    dq.appendleft(chunk[remaining:])
                    chunk = chunk[:remaining]
                prefix.append(chunk)
                remaining -= len(chunk)

            buff = b''.join(prefix)
        return buff

    def _handle_events(self, fd, events):
        # This is the actual callback from the event loop
        if not self.sock:
            self.log.warning("Got events for closed stream %d", fd)
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


class BaseSocketHandler(BaseSocket):
    '''A :class:`BaseSocket` class for all socket handlers such as
:class:`AsyncSocketServer`, :class:`AsyncConnection`.

.. attribute:: on_closed

    A :class:`Deferred` which receives a callback once the
    :meth:`close` method is invoked
.. '''
    _closing_socket = False
    def __new__(cls, *args, **kwargs):
        o = super(BaseSocketHandler, cls).__new__(cls)
        o.on_closed = Deferred(
                        description='on_closed BaseSocketHandler callback')
        o.time_started = time.time()
        o.time_last = o.time_started
        o.received = 0
        return o

    @property
    def closed(self):
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
                    self.log.info('Closing %s on timeout.', self)
            else:
                log_failure(msg)
        self.on_close(msg)
        self.sock.close()
        return self.on_closed.callback(msg)


class EchoParser:
    '''A simple echo parser'''
    def encode(self, data):
        return data
    
    def decode(self, data):
        return bytes(data), bytearray()
    
    
class ClientSocketHandler(BaseSocketHandler):
    '''Base class for socket clients with parsers. This class can be used for
synchronous and asynchronous socket for both a "client" socket and
the server connection socket (the socket obtained from a server socket
via the ``connect`` function).'''
    parser_class = EchoParser
    log = iologger
    def __init__(self, socket, address, parser_class=None, timeout=None):
        '''Create a client or client-connection socket. A parser class
is required in order to use :class:`SocketClient`.

:parameter socket: a client or client-connection socket
:parameter address: The address of the remote client/server
:parameter parser_class: A class used for parsing messages.
:parameter timeout: A timeout in seconds for the socket. Same rules as
    the ``socket.settimeout`` method in the standard library.
'''
        self._socket_timeout = get_socket_timeout(timeout)
        self._set_socket(socket)
        self.remote_address = address
        parser_class = parser_class or self.parser_class
        self.parser = parser_class()
        self.buffer = bytearray()

    def __repr__(self):
        return str(self.remote_address)
    __str__ = __repr__

    def _set_socket(self, sock):
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


class ClientSocket(ClientSocketHandler, IOClientRead):
    '''Synchronous/Asynchronous client for a remote socket server. This client
maintain a connection with remote server and exchange data by writing and
reading from the same socket connection.'''
    def __init__(self, *args, **kwargs):
        super(ClientSocket, self).__init__(*args, **kwargs)
        self.exec_queue = deque()
        self.processing = None
        
    @classmethod
    def connect(cls, address, parser_class=None, timeout=None):
        sock = create_connection(address)
        return cls(sock, address, parser_class=parser_class, timeout=timeout)

    def send(self, data):
        '''Send data to remote server'''
        self.time_last = time.time()
        data = self.parser.encode(data)
        return self.sock.write(data)
        
    def execute(self, data):
        '''Send and read data from socket. It makes sure commands are queued
and executed in an orderly fashion. For asynchronous connection it returns
a :class:`Deferred` called once data has been received from the server and
parsed.'''
        if self.async:
            cbk = Deferred()
            if data:
                self.exec_queue.append((data, cbk))
                cbk.addBoth(self._got_result)
                self._consume_next()
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
        if not self.processing and self.exec_queue:
            self.processing = True
            data, cbk = self.exec_queue.popleft()
            msg = self.send(data)
            if is_async(msg):
                msg.add_callback(self._read, self.close)
            else:
                msg = self._read()
            msg = maybe_async(msg)
            if is_async(msg):
                msg.addBoth(cbk.callback)
            else:
                cbk.callback(msg)
            
    def _got_result(self, result):
        # Got the result, ping the consumer so it can process
        # requests from the queue
        self.processing = False
        self._consume_next()
        return result

    def _parsedata(self, data):
        buffer = self.buffer
        if data:
            buffer.extend(data)
        if not buffer:
            return
        try:
            parsed_data, buffer = self.parser.decode(buffer)
        except CouldNotParse:
            self.log.warn('Could not parse data', exc_info=True)
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
    '''An asynchronous response is created once a connection has produced
finished data from a read operation. Instances of this class are iterable over
chunk of data to send back to the remote client.

.. attribute:: connection

    The :class:`AsyncConnection` for this response

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
    def parser(self):
        return self.connection.parser

    @property
    def sock(self):
        return self.connection.sock

    def __iter__(self):
        yield self.parsed_data


class AsyncConnection(ClientSocketHandler):
    '''An asynchronous client connection for a :class:`AsyncSocketServer`.
The connection maintains the client socket open for as long as it is required.
A connection can handle several request/responses until it is closed.

.. attribute:: server

    The class :class:`AsyncSocketServer` which created the connection

'''
    response_class = AsyncResponse
    '''Class or callable for building an :class:`AsyncResponse` object.'''

    def __init__(self, sock, address, server, timeout=None):
        if not isinstance(sock, AsyncIOStream):
            sock = AsyncIOStream(sock, timeout=server.timeout)
        super(AsyncConnection, self).__init__(sock, address,
                                              server.parser_class)
        self.server = server
        server.connections.add(self)
        self.handle().add_errback(self.close)

    @async(max_errors=1, description='Async client connection generator')
    def handle(self):
        while not self.closed:
            outcome = self.sock.read()
            yield outcome
            # if we sare here it means no errors occurred so far
            self.time_last = time.time()
            buffer = self.buffer
            buffer.extend(outcome.result)
            parsed_data = True
            # IMPORTANT! Consume all data until the parser returns nothing.
            while parsed_data:
                parsed_data = self.request_data()
                if parsed_data:
                    self.received += 1
                    # The response is iterable (same as WSGI response)
                    for data in self.response_class(self, parsed_data):
                        if data:
                            yield self.write(data)
                        else: # The response is not ready. release the loop
                            yield NOT_DONE

    def _get_timeout(self):
        return self.sock._read_callback_timeout
    def _set_timeout(self, value):
        self.sock._read_callback_timeout = value
    read_timeout = property(_get_timeout, _set_timeout) 
    
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
        self.parser.connection = self
        try:
            parsed_data, buffer = self.parser.decode(buffer)
        except CouldNotParse:
            self.log.warn('Could not parse data', exc_info=True)
            parsed_data = None
            buffer = bytearray()
        self.buffer = buffer
        return parsed_data

    @property
    def actor(self):
        return self.server.actor

    @property
    def log(self):
        return self.actor.log

    def on_close(self, failure=None):
        self.server.connections.discard(self)

    def write(self, data):
        '''Write data to socket.'''
        return self.sock.write(data)


class AsyncSocketServer(BaseSocketHandler):
    '''A :class:`BaseSocketHandler` for asynchronous socket servers.

.. attribute:: actor

    The :class:`Actor` powering this :class:`AsyncSocketServer`.

.. attribute:: ioloop

    The :class:`IOLoop` used by this :class:`AsyncSocketServer` for
    asynchronously sending and receiving data.

.. attribute:: connections

    The set of all open :class:`AsyncConnection`

.. attribute:: onthread

    If ``True`` the server has its own :class:`IOLoop` running on a separate
    thread of execution. Otherwise it shares the :attr:`actor.requestloop`

.. attribute:: parser_class

    A class for encoding and decoding data

.. attribute:: timeout

    The timeout for when reading data in an asynchronous way.
'''
    thread = None
    _started = False
    connection_class = AsyncConnection
    parser_class = EchoParser
    def __init__(self, actor, socket, parser_class=None, onthread=False,
                 connection_class=None, timeout=None):
        self.actor = actor
        self.parser_class = parser_class or self.parser_class
        self.connection_class = connection_class or self.connection_class
        self.sock = wrap_socket(socket)
        self.connections = set()
        self.onthread = onthread
        self.timeout = timeout
        self.on_connection_callbacks = []
        # If the actor has a ioqueue (CPU bound actor) we create a new ioloop
        if self.onthread:
            self.__ioloop = IOLoop(pool_timeout=actor._pool_timeout,
                                   logger=actor.log)

    @classmethod
    def make(cls, actor=None, bind=None, backlog=None, **kwargs):
        if actor is None:
            actor = get_actor()
        if not backlog:
            if actor:
                backlog = actor.cfg.get('backlog', defaults.BACKLOG)
            else:
                backlog = defaults.BACKLOG
        if bind:
            socket = create_socket(bind, backlog=backlog)
        else:
            socket = server_socket(backlog=backlog)
        return cls(actor, socket, **kwargs)

    @property
    def name(self):
        return '%s %s' % (self.actor, self.address)
    
    @property
    def started(self):
        return self._started

    def __repr__(self):
        return self.name
    __str__ = __repr__

    def start(self):
        self.actor.requestloop.add_callback(self._start)
        return self

    @property
    def ioloop(self):
        return self.__ioloop if self.onthread else self.actor_ioloop()

    @property
    def active_connections(self):
        return len(self.connections)
    
    def actor_ioloop(self):
        return self.actor.ioloop

    def on_start(self):
        '''callback just before the event loop starts.'''
        pass

    def shut_down(self):
        pass

    def quit_connections(self):
        for c in list(self.connections):
            c.close()
            
    def on_close(self, failure=None):
        self.shut_down()
        self.quit_connections()
        self.ioloop.remove_handler(self)
        if self.onthread:
            self.ioloop.stop()
            # We join the thread
            if current_thread() != self.thread:
                self.thread.join()

    ############################################################## INTERNALS
    def _start(self):
        # If onthread is true we start the event loop on a
        # separate thread.
        if self.onthread:
            if self.thread and self.thread.is_alive():
                raise RunTimeError('Cannot start socket server. '\
                                   'It has already started')
            self.thread = PulsarThread(name=self.name, target=self._run)
            self.thread.start()
        if not self._started:
            self._started = True
            self.ioloop.add_handler(self,
                                    self.new_connection,
                                    self.ioloop.READ)
            return self.on_start()

    def _run(self):
        self.ioloop.start()

    def new_connection(self, fd, events):
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
            return self.connection_class(client, client_address, self,
                                         timeout=self.timeout)
