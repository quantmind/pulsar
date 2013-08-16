'''TCP protocol clients and servers'''
import socket

from pulsar.utils.sockets import *
from pulsar.utils.pep import range
from pulsar.utils.structures import merge_prefix

from .servers import Server
from .transport import Transport

__all__ = ['TCP']

TRY_WRITE_AGAIN = (EWOULDBLOCK, ENOBUFS, EINPROGRESS)
TRY_READ_AGAIN = (EWOULDBLOCK, EAGAIN)
NUMBER_ACCEPTS = 30 if platform.type == "posix" else 1


class SocketTransport(Transport):
    
    def __init__(self, event_loop, sock, protocol, extra=None):
        self._protocol = protocol
        self._sock = sock
        self._event_loop = event_loop
        event_loop.add_reader(sock.fileno(), self._read_ready)
    

def start_serving(event_loop, protocol_factory, host=None, port=None,
                  family=socket.AF_UNSPEC, flags=socket.AI_PASSIVE,
                  sock=None, backlog=100, ssl=None, reuse_address=None):
    #Coroutine to start serving TCP streams. This function is called by
    #event_loop
    if host is not None or port is not None:
        if sock is not None:
            raise ValueError(
                    'host/port and sock can not be specified at the same time')
        #
        AF_INET6 = getattr(socket, 'AF_INET6', 0)
        if reuse_address is None:
            reuse_address = os.name == 'posix' and sys.platform != 'cygwin'
        
        infos = yield event_loop.getaddrinfo(host, port, family=family,
                                             type=socket.SOCK_STREAM, proto=0,
                                             flags=flags)
        if not infos:
            raise socket.error('getaddrinfo() returned empty list')
        completed = False
        sockets = []
        try:
            for family, socktype, proto, canonname, address in infos:
                sock = socket.socket(af, socktype, proto)
                sockets.append(sock)
                if reuse_address:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                    True)
                # Disable IPv4/IPv6 dual stack support (enabled by
                # default on Linux) which makes a single socket
                # listen on both address families.
                if family == AF_INET6 and hasattr(socket, 'IPPROTO_IPV6'):
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY,
                                    True)
                try:
                    sock.bind(address)
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
    #
    for sock in sockets:
        sock.listen(backlog)
        sock.setblocking(False)
        event_loop.add_reader(sock.fileno(), accept_connection,
                              event_loop, protocol_factory, sock, ssl)
    return sockets


def accept_connection(event_loop, protocol_factory, sock, ssl):
    for i in range(NUMBER_ACCEPTS):
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
        except socket.error as e:
            if e.args[0] in TRY_READ_AGAIN:
                break
            elif e.args[0] == EPERM:
                # Netfilter on Linux may have rejected the
                # connection, but we get told to try to accept() anyway.
                continue
            elif e.args[0] in TCP_ACCEPT_ERRORS:
                event_loop.logger.info('Could not accept new connection')
                break
            raise
        except Exception:
            event_loop.remove_reader(sock.fileno())
            sock.close()
            event_loop.logger.exception('Could not accept new connection')
            break
        else:
            protocol = protocol_factory()
            if ssl:
                SocketSslTransport(event_loop, conn, protocol,
                                   ssl, None, server_side=True,
                                   extra={'addr': addr})
            else:
                SocketTransport(event_loop, conn, protocol,
                                extra={'addr': addr})


class TCP(SocketTransport):
    '''Transport for the TCP protocol.'''
    TYPE = socket.SOCK_STREAM
    
    def _protocol_connect(self, address):
        #return self.event_loop.create_connection()
        try:
            self.sock.connect(address)
        except socket.error as e:
            if e.args[0] in TRY_WRITE_AGAIN:
                return False
            else:
                raise
        else:
            return True #    A synchronous connection
    
    def _protocol_read(self):
        try:
            return self._sock.recv(self._read_chunk_size)
        except socket.error as e:
            if e.args[0] == EWOULDBLOCK:
                return
            else:
                raise
            
    def _protocol_write(self):
        buffer = self._write_buffer
        tot_bytes = 0
        if not buffer:
            LOGGER.warning('handling write on a 0 length buffer')
        while buffer:
            try:
                sent = self.sock.send(buffer[0])
                if sent == 0:
                    # With OpenSSL, after send returns EWOULDBLOCK,
                    # the very same string object must be used on the
                    # next call to send.  Therefore we suppress
                    # merging the write buffer after an EWOULDBLOCK.
                    break
                merge_prefix(buffer, sent)
                buffer.popleft()
                tot_bytes += sent
            except socket.error as e:
                if e.args[0] in TRY_WRITE_AGAIN:
                    break
                else:
                    raise
        return tot_bytes
        
    def _protocol_accept(self):
        try:
            for i in range(NUMBER_ACCEPTS):
                if self.closed:
                    return
                try:
                    sock, address = self.sock.accept()
                except socket.error as e:
                    if e.args[0] in TRY_READ_AGAIN:
                        break
                    elif e.args[0] == EPERM:
                        # Netfilter on Linux may have rejected the
                        # connection, but we get told to try to accept() anyway.
                        continue
                    elif e.args[0] in TCP_ACCEPT_ERRORS:
                        LOGGER.info('Could not accept new connection')
                        break
                    raise
                self._handle_accept(sock, address)
        except Exception:
            LOGGER.exception('Could not accept new connection')
        
    def _handle_accept(self, sock, address):
        self._protocol.data_received(sock, address)