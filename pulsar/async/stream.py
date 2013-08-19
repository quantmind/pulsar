import os
import sys
import socket
try:
    import ssl
except ImportError:  # pragma: no cover
    ssl = None

from pulsar.utils.internet import (TCP_TRY_WRITE_AGAIN, TCP_TRY_READ_AGAIN,
                                   TCP_ACCEPT_ERRORS, EWOULDBLOCK, EPERM)
from .consts import NUMBER_ACCEPTS
from .defer import multi_async, Deferred
from .internet import SocketTransport

AF_INET6 = getattr(socket, 'AF_INET6', 0)


class SocketStreamTransport(SocketTransport):
    
    def start(self):
        loop = self._event_loop
        loop.add_reader(self._sock_fd, self._read_ready)
        self._loop.call_soon(self._protocol.connection_made, self)
    
    def _ready_read(self):
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # If any other error occur, abort the connection and re-raise.
        passes = 0
        chunk = True
        while chunk:
            try:
                try:
                    chunk = self._sock.recv(self._read_chunk_size)
                except socket.error as e:
                    if e.args[0] == EWOULDBLOCK:
                        return
                    else:
                        raise
                if chunk:
                    if self._paused:
                        self._read_buffer.append(data)
                    else:
                        self._protocol.data_received(data)
                elif not passes and chunk == b'':
                    # We got empty data. Close the socket
                    try:
                        self._protocol.eof_received()
                    finally:
                        self.close()
                passes += 1
            except Exception as exc:
                self.abort(exc)
                raise
    
    
class SocketStreamSslTransport(SocketStreamTransport):
    
    def __init__(self, event_loop, rawsock, protocol, sslcontext,
                 server_side=False, **kwargs):
        if server_side:
            assert isinstance(
                sslcontext, ssl.SSLContext), 'Must pass an SSLContext'
        else:
            # Client-side may pass ssl=True to use a default context.
            sslcontext = sslcontext or ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        sslsock = sslcontext.wrap_socket(rawsock, server_side=server_side,
                                         do_handshake_on_connect=False)

        super(SocketStreamSslTransport, self).__init__(loop, sslsock, protocol,
                                                       **kwargs)
        self._rawsock = rawsock


def create_connection(event_loop, protocol_factory, host, port, ssl,
                      family, proto, flags, sock, local_addr):
    if host is not None or port is not None:
        if sock is not None:
            raise ValueError(
                'host/port and sock can not be specified at the same time')

        fs = [event_loop.getaddrinfo(
                host, port, family=family,
                type=socket.SOCK_STREAM, proto=proto, flags=flags)]
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
                sock.setblocking(False)
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
            event_loop, sock, protocol, sslcontext, waiter, server_side=False)
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
            event_loop.remove_writer(fd)
            d._suppressAlreadyCalled = True
        #
        future = Deferred(canceller=canceller, event_loop=event_loop)
        connect = True
    try:
        if connect:
            try:
                sock.connect(address)
            except socket.error as e:
                if e.args[0] in TCP_TRY_WRITE_AGAIN:
                    event_loop.add_writer(fd, sock_connect, event_loop, sock,
                                          address, future)
                    return future
                else:
                    raise
        else:   # This is the callback from the event loop
            event_loop.remove_writer(fd)
            if future.cancelled():
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                # Jump to the except clause below.
                raise socket.error(err, 'Connect call failed')
            future.callback(None)
    except Exception as exc:
        return future.callback(exc)
        
def sock_accept(event_loop, sock, future=None):
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
            if e.args[0] in TCP_TRY_READ_AGAIN:
                event_loop.add_reader(fd, sock_accept, event_loop, sock, future)
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
    try:
        for i in range(NUMBER_ACCEPTS):
            try:
                conn, address = sock.accept()
            except socket.error as e:
                if e.args[0] in TCP_TRY_READ_AGAIN:
                    break
                elif e.args[0] == EPERM:
                    # Netfilter on Linux may have rejected the
                    # connection, but we get told to try to accept() anyway.
                    continue
                elif e.args[0] in TCP_ACCEPT_ERRORS:
                    event_loop.logger.info('Could not accept new connection')
                    break
                raise
            protocol = protocol_factory()
            if ssl:
                transport = StreamSslTransport(event_loop, conn, protocol, ssl)
            else:
                transport = StreamTransport(event_loop, conn, protocol)
            transport.start()
    except Exception:
        event_loop.logger.exception('Could not accept new connection')