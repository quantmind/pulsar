'''
The :mod:`pulsar.async.stream` implements classes and functions for
handling TCP streams.

SocketStreamTransport
============================

.. autoclass:: SocketStreamTransport
   :members:
   :member-order: bysource

TcpServer
============================

.. autoclass:: TcpServer
   :members:
   :member-order: bysource
'''
import os
import sys
import socket
from functools import partial

from pulsar.utils.exceptions import PulsarException
from pulsar.utils.internet import (TRY_WRITE_AGAIN, TRY_READ_AGAIN,
                                   ACCEPT_ERRORS, EWOULDBLOCK, EPERM,
                                   format_address, ssl_context, ssl,
                                   ESHUTDOWN, BUFFER_MAX_SIZE,
                                   SOCKET_INTERRUPT_ERRORS)
from pulsar.utils.structures import merge_prefix

from .consts import NUMBER_ACCEPTS
from .defer import multi_async, Deferred
from .internet import SocketTransport, AF_INET6
from .protocols import Server, logger

SSLV3_ALERT_CERTIFICATE_UNKNOWN = 1
# Got this error on pypy
SSL3_WRITE_PENDING = 1
MAX_CONSECUTIVE_WRITES = 500


class TooManyConsecutiveWrite(PulsarException):
    '''Raise when too many consecutive writes are attempted.'''


def raise_socket_error(e):
    eno = getattr(e, 'errno', None)
    if eno not in SOCKET_INTERRUPT_ERRORS:
        args = getattr(e, 'args', None)
        if isinstance(args, tuple) and len(args) == 2:
            eno = args[0]
    return eno not in SOCKET_INTERRUPT_ERRORS


class SocketStreamTransport(SocketTransport):
    '''A :class:`pulsar.SocketTransport` for TCP streams.

The primary feature of a stream transport is sending bytes to a protocol
and receiving bytes from the underlying protocol. Writing to the transport
is done using the :meth:`write` and :meth:`writelines` methods.
The latter method is a performance optimisation, to allow software to take
advantage of specific capabilities in some transport mechanisms.'''
    _paused_reading = False

    def _do_handshake(self):
        self._event_loop.add_reader(self._sock_fd, self._ready_read)
        self._event_loop.call_soon(self._protocol.connection_made, self)

    def pause_reading(self):
        '''Suspend delivery of data to the protocol until a subsequent
        :meth:`resume_reading` call.

        Between :meth:`pause_reading` and :meth:`resume_reading`, the
        protocol's data_received() method will not be called.
        '''
        if self._closing:
            raise RuntimeError('Cannot pause_reading() when closing')
        if self._paused_reading:
            raise RuntimeError('Already paused')
        self._paused_reading = True
        self._loop.remove_reader(self._sock_fd)

    def resume_reading(self):
        """Resume the receiving end."""
        if not self._paused_reading:
            raise RuntimeError('Not paused')
        self._paused_reading = False
        if not self._closing:
            self._loop.add_reader(self._sock_fd)

    def write(self, data):
        '''Write chunk of ``data`` to the endpoint.
        '''
        if not data:
            return
        self._check_closed()
        is_writing = bool(self._write_buffer)
        if data:
            # Add data to the buffer
            assert isinstance(data, bytes)
            if len(data) > BUFFER_MAX_SIZE:
                for i in range(0, len(data), BUFFER_MAX_SIZE):
                    self._write_buffer.append(data[i:i+BUFFER_MAX_SIZE])
            else:
                self._write_buffer.append(data)
        # Try to write only when not waiting for write callbacks
        if not is_writing:
            self._consecutive_writes = 0
            self._ready_write()
            if self._write_buffer:    # still writing
                self._event_loop.add_writer(self._sock_fd, self._ready_write)
            elif self._closing:
                self._event_loop.call_soon(self._shutdown)
        else:
            self._consecutive_writes += 1
            if self._consecutive_writes > MAX_CONSECUTIVE_WRITES:
                self.abort(TooManyConsecutiveWrite())

    def writelines(self, list_of_data):
        '''Write a list (or any iterable) of data bytes to the transport.
        '''
        self.write(b''.join(list_of_data))

    ##    INTERNALS

    def _write_continue(self, e):
        return e.args[0] in TRY_WRITE_AGAIN

    def _read_continue(self, e):
        return e.args[0] == EWOULDBLOCK

    def _ready_write(self):
        # Do the actual writing
        buffer = self._write_buffer
        tot_bytes = 0
        if not buffer:
            self.logger.warning('handling write on a 0 length buffer')
        try:
            while buffer:
                try:
                    sent = self._sock.send(buffer[0])
                    if sent == 0:
                        break
                    merge_prefix(buffer, sent)
                    buffer.popleft()
                    tot_bytes += sent
                except self.SocketError as e:
                    if self._write_continue(e):
                        break
                    else:
                        raise
        except Exception:
            failure = sys.exc_info()
        else:
            if not self._write_buffer:
                self._event_loop.remove_writer(self._sock_fd)
                if self._closing:
                    self._event_loop.call_soon(self._shutdown)
            return tot_bytes
        if not self._closing:
            self.abort(failure)

    def _ready_read(self):
        try:
            try:
                chunk = self._sock.recv(self._read_chunk_size)
            except self.SocketError as e:
                if self._read_continue(e):
                    return
                if raise_socket_error(e):
                    raise
                else:
                    chunk = None
            if chunk:
                if self._paused_reading:
                    self._read_buffer.append(chunk)
                else:
                    self._protocol.data_received(chunk)
            else:
                # We got empty data. Close the socket
                try:
                    self._protocol.eof_received()
                finally:
                    self.close()
            return
        except self.SocketError:
            failure = None if self._closing else sys.exc_info()
        except Exception:
            failure = sys.exc_info()
        if failure:
            self.abort(failure)

    def mute_read_error(self, error):
        '''Return ``True`` if a socket error from a read operation is muted.

        This is when the socket cannot send sinse the socket has started
        shutting down already.
        '''
        return error.errno in (ESHUTDOWN,)


class SocketStreamSslTransport(SocketStreamTransport):
    SocketError = getattr(ssl, 'SSLError', None)

    def __init__(self, event_loop, rawsock, protocol, sslcontext,
                 server_side=True, server_hostname=None, **kwargs):
        sslcontext = ssl_context(sslcontext, server_side=server_side)
        sslsock = sslcontext.wrap_socket(rawsock, server_side=server_side,
                                         do_handshake_on_connect=False,
                                         server_hostname=server_hostname)
        self._rawsock = rawsock
        # waiting for reading handshake
        self._handshake_reading = False
        # waiting for writing handshake
        self._handshake_writing = False
        super(SocketStreamSslTransport, self).__init__(event_loop, sslsock,
                                                       protocol, **kwargs)

    @property
    def rawsock(self):
        '''The raw socket.

        This is the socket not wrapped by the sslcontext.
        '''
        return self._rawsock

    def _write_continue(self, e):
        return e.errno in (ssl.SSL_ERROR_WANT_WRITE,
                           SSL3_WRITE_PENDING)

    def _read_continue(self, e):
        return e.errno in (SSLV3_ALERT_CERTIFICATE_UNKNOWN,
                           ssl.SSL_ERROR_WANT_READ)

    def _do_handshake(self):
        loop = self._event_loop
        try:
            self._sock.do_handshake()
        except self.SocketError as e:
            if self._read_continue(e):
                loop.remove_reader(self._sock_fd)
                self._handshake_reading = True
                loop.add_reader(self._sock_fd, self._do_handshake)
                return
            elif self._write_continue(e):
                loop.remove_writer(self._sock_fd)
                self._handshake_writing = True
                loop.add_writer(self._sock_fd, self._do_handshake)
                return
            else:
                raise
        except Exception:
            failure = sys.exc_info()
        else:
            loop = self._event_loop
            if self._handshake_reading:
                loop.remove_reader(self._sock_fd)
                loop.add_reader(self._sock_fd, self._ready_read)
            else:
                if self._handshake_writing:
                    loop.remove_writer(self._sock_fd)
                loop.add_reader(self._sock_fd, self._ready_read)
                loop.add_writer(self._sock_fd, self._ready_write)
            self._handshake_reading = False
            self._handshake_writing = False
            self._event_loop.call_soon(self._protocol.connection_made, self)
            return
        self.abort(failure)


class TcpServer(Server):
    '''A TCP :class:`pulsar.Server`.

    .. attribute:: consumer_factory

        Callable or a :class:`pulsar.ProtocolConsumer` class for producing
        :class:`ProtocolConsumer` which handle the receiving, decoding and
        sending of data.

    '''
    def start_serving(self, backlog=100, sslcontext=None):
        '''Start serving the Tcp socket.

        :param backlog: Number of maximum connections
        :param sslcontext: optional SSLContext object.
        :return: a :class:`pulsar.Deferred` called back when the server is
            serving the socket.'''
        if not self.event('start').done():
            res = self._event_loop.start_serving(self.protocol_factory,
                                                 host=self._host,
                                                 port=self._port,
                                                 sock=self._sock,
                                                 backlog=backlog,
                                                 ssl=sslcontext)
            return res.add_callback(self._got_sockets
                                    ).add_both(partial(self.fire_event,
                                                       'start'))

    def stop_serving(self):
        '''Stop serving the :class:`pulsar.Server.sock`'''
        if self._sock:
            sock, self._sock = self._sock, None
            self._event_loop.stop_serving(sock)

    def close(self):
        '''Same as :meth:`stop_serving` method.'''
        if self._sock:
            sock, self._sock = self._sock, None
            self._event_loop.stop_serving(sock)
            self._event_loop.call_soon(self._close)
        self.stop_serving()

    def _got_sockets(self, sockets):
        self._sock = sockets[0]
        self.logger.info('%s serving on %s', self._name,
                         format_address(self.address))
        return self

    def _close(self):
        yield self.close_connections()
        self.fire_event('stop')
        yield self


def create_connection(event_loop, protocol_factory, host, port, ssl,
                      family, proto, flags, sock, local_addr):
    if host is not None or port is not None:
        if sock is not None:
            raise ValueError(
                'host/port and sock can not be specified at the same time')
        fs = [event_loop.getaddrinfo(host, port, family=family,
                                     type=socket.SOCK_STREAM, proto=proto,
                                     flags=flags)]
        if local_addr is not None:
            fs.append(event_loop.getaddrinfo(
                *local_addr, family=family,
                type=socket.SOCK_STREAM, proto=proto, flags=flags))
        #
        fs = yield multi_async(fs)
        if len(fs) == 2:
            laddr_infos = fs[1]
            if not laddr_infos:
                raise socket.error('getaddrinfo() returned empty list')
        else:
            laddr_infos = None
        #
        socket_factory = getattr(event_loop, 'socket_factory', socket.socket)
        exceptions = []
        for family, type, proto, cname, address in fs[0]:
            try:
                sock = socket_factory(family=family, type=type, proto=proto)
                sock.setblocking(0)
                if laddr_infos is not None:
                    for _, _, _, _, laddr in laddr_infos:
                        try:
                            sock.bind(laddr)
                            break
                        except socket.error as exc:
                            exc = socket.error(
                                exc.errno, 'error while '
                                'attempting to bind on address '
                                '{!r}: {}'.format(
                                    laddr, exc.strerror.lower()))
                            exceptions.append(exc)
                    else:
                        sock.close()
                        sock = None
                        continue
                yield event_loop.sock_connect(sock, address)
            except socket.error as exc:
                if sock is not None:
                    sock.close()
                exceptions.append(exc)
            else:
                break
        else:
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                # If they all have the same str(), raise one.
                model = str(exceptions[0])
                if all(str(exc) == model for exc in exceptions):
                    raise exceptions[0]
                # Raise a combined exception so the user can see all
                # the various error messages.
                raise socket.error('Multiple exceptions: {}'.format(
                    ', '.join(str(exc) for exc in exceptions)))

    elif sock is None:
        raise ValueError(
            'host and port was not specified and no sock specified')

    sock.setblocking(False)
    protocol = protocol_factory()
    if ssl:
        sslcontext = None if isinstance(ssl, bool) else ssl
        transport = SocketStreamSslTransport(
            event_loop, sock, protocol, sslcontext, server_side=False)
    else:
        transport = SocketStreamTransport(event_loop, sock, protocol)
    yield transport, protocol


def start_serving(event_loop, protocol_factory, host, port, ssl,
                  family, flags, sock, backlog, reuse_address):
    #Coroutine which starts socket servers
    if host is not None or port is not None:
        if sock is not None:
            raise ValueError(
                'host/port and sock can not be specified at the same time')
        if reuse_address is None:
            reuse_address = os.name == 'posix' and sys.platform != 'cygwin'
        sockets = []
        if host == '':
            host = None

        infos = yield event_loop.getaddrinfo(
            host, port, family=family,
            type=socket.SOCK_STREAM, proto=0, flags=flags)
        if not infos:
            raise socket.error('getaddrinfo() returned empty list')
        #
        socket_factory = getattr(event_loop, 'socket_factory', socket.socket)
        completed = False
        try:
            for af, socktype, proto, canonname, sa in infos:
                sock = socket_factory(af, socktype, proto)
                sockets.append(sock)
                if reuse_address:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                    True)
                # Disable IPv4/IPv6 dual stack support (enabled by
                # default on Linux) which makes a single socket
                # listen on both address families.
                if af == AF_INET6 and hasattr(socket, 'IPPROTO_IPV6'):
                    sock.setsockopt(socket.IPPROTO_IPV6,
                                    socket.IPV6_V6ONLY,
                                    True)
                try:
                    sock.bind(sa)
                except socket.error as err:
                    raise socket.error(err.errno, 'error while attempting '
                                       'to bind on address %r: %s'
                                       % (sa, err.strerror.lower()))
            completed = True
        finally:
            if not completed:
                for sock in sockets:
                    sock.close()
    else:
        if sock is None:
            raise ValueError(
                'host and port was not specified and no sock specified')
        sockets = [sock]

    for sock in sockets:
        sock.listen(backlog)
        sock.setblocking(False)
        event_loop.add_reader(sock.fileno(), sock_accept_connection,
                              event_loop, protocol_factory, sock, ssl)
    yield sockets


def sock_connect(event_loop, sock, address, future=None):
    fd = sock.fileno()
    connect = False
    if future is None:
        def canceller(d):
            event_loop.remove_connector(fd)
            d._suppressAlreadyCalled = True
        #
        future = Deferred(canceller=canceller, event_loop=event_loop)
        connect = True
    try:
        if connect:
            try:
                sock.connect(address)
            except socket.error as e:
                if e.args[0] in TRY_WRITE_AGAIN:
                    event_loop.add_connector(fd, sock_connect, event_loop,
                                             sock, address, future)
                    return future
                else:
                    raise
        else:   # This is the callback from the event loop
            event_loop.remove_connector(fd)
            if future.cancelled():
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                # Jump to the except clause below.
                raise socket.error(err, 'Connect call failed')
            future.callback(None)
    except Exception as exc:
        return future.callback(exc)


def sock_accept(event_loop, sock, future=None):    # pragma    nocover
    #TODO
    #do we need this function?
    fd = sock.fileno()
    if future is None:
        future = Deferred()
    else:
        event_loop.remove_reader(fd)
    if not future.cancelled():
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
            future.set_result((conn, address))
        except socket.error as e:
            if e.args[0] in TRY_READ_AGAIN:
                event_loop.add_reader(fd, sock_accept, event_loop, sock,
                                      future)
            elif e.args[0] == EPERM:
                # Netfilter on Linux may have rejected the
                # connection, but we get told to try to accept() anyway.
                return sock_accept(event_loop, sock, future)
            else:
                future.callback(e)
        except Exception as e:
            future.callback(e)
    return future


def sock_accept_connection(event_loop, protocol_factory, sock, ssl):
    '''Used by start_serving.'''
    try:
        for i in range(NUMBER_ACCEPTS):
            try:
                conn, address = sock.accept()
            except socket.error as e:
                if e.args[0] in TRY_READ_AGAIN:
                    break
                elif e.args[0] == EPERM:
                    # Netfilter on Linux may have rejected the
                    # connection, but we get told to try to accept() anyway.
                    continue
                elif e.args[0] in ACCEPT_ERRORS:
                    logger(event_loop).info(
                        'Could not accept new connection %s: %s', i+1, e)
                    break
                raise
            protocol = protocol_factory()
            if ssl:
                SocketStreamSslTransport(event_loop, conn, protocol, ssl,
                                         extra={'addr': address})
            else:
                SocketStreamTransport(event_loop, conn, protocol,
                                      extra={'addr': address})
    except Exception:
        logger(event_loop).exception('Could not accept new connection')
