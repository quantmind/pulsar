import socket

from pulsar.utils.structures import OrderedDict 
from pulsar.utils.internet import (TRY_WRITE_AGAIN, TRY_READ_AGAIN,
                                   ECONNREFUSED, WRITE_BUFFER_MAX_SIZE)

from .internet import SocketTransport
from .consts import LOG_THRESHOLD_FOR_CONNLOST_WRITES


class SocketDatagramTransport(SocketTransport):
    
    def __init__(self, event_loop, sock, protocol, address=None, **kwargs):
        self._address = address
        super(SocketDatagramTransport, self).__init__(
                                event_loop, sock, protocol, **kwargs)
    
    def _do_handshake(self):
        self._event_loop.add_reader(self._sock_fd, self._ready_read)
        self._event_loop.call_soon(self._protocol.connection_made, self)
    
    def _check_closed(self):
        if self._conn_lost:
            if self._conn_lost >= LOG_THRESHOLD_FOR_CONNLOST_WRITES:
                self.logger.warning('socket.send() raised exception.')
            self._conn_lost += 1
        return self._conn_lost
        
    def sendto(self, data, addr=None):
        '''Send chunk of ``data`` to the endpoint.'''
        if not data:
            return
        if self._address:
            assert addr in (None, self._address)
        if self._check_closed():
            return
        writing = self.writing
        if data:
            assert isinstance(data, bytes)
            if len(data) > WRITE_BUFFER_MAX_SIZE:
                for i in range(0, len(data), WRITE_BUFFER_MAX_SIZE):
                    chunk = data[i:i+WRITE_BUFFER_MAX_SIZE]
                    self._write_buffer.append((chunk, addr))
            else:
                self._write_buffer.append((data, addr))
        # Try to write only when not waiting for write callbacks
        if not writing:
            self._ready_sendto()
            if self.writing:    # still writing
                self._event_loop.add_writer(self._sock_fd, self._ready_sendto)
            elif self._closing:
                self._event_loop.call_soon(self._shutdown)
                
    def _ready_read(self):
        # Read from the socket until we get EWOULDBLOCK or equivalent.
        # If any other error occur, abort the connection and re-raise.
        passes = 0
        chunk = True
        while chunk:
            try:
                try:
                    data, addr = self._sock.recvfrom(self.max_packet_size)
                except socket.error as e:
                    if e.args[0] in TRY_READ_AGAIN:
                        return
                    else:
                        raise
                if chunk:
                    if self._paused:
                        self._read_buffer.append((data, addr))
                    else:
                        self._protocol.datagram_received(data, addr)
                passes += 1
            except Exception as exc:
                self.abort(exc)
                
    def _ready_sendto(self):
        # Do the actual writing
        buffer = self._write_buffer
        tot_bytes = 0
        if not buffer:
            self.logger.warning('handling write on a 0 length buffer')
        try:
            while buffer:
                data, addr = buffer[0]
                try:
                    if self._address:
                        sent = self._sock.send(data)
                    else:
                        sent = self._sock.sendto(data, addr)
                    buffer.popleft()
                    if sent < len(data):
                        buffer.appendleft((data[sent:], addr))
                    tot_bytes += sent
                except self.SocketError as e:
                    if e.args[0] in TRY_WRITE_AGAIN:
                        break
                    elif e.args[0] == ECONNREFUSED and not self._address:
                        break
                    else:
                        raise
        except Exception as e:
            self.abort(exc=e)
        else:
            if not self.writing:
                self._event_loop.remove_writer(self._sock_fd)
                if self._closing:
                    self._event_loop.call_soon(self._shutdown)
            return tot_bytes
                
                
def create_datagram_endpoint(event_loop, protocol_factory, local_addr,
                             remote_addr, family, proto, flags):
    if not (local_addr or remote_addr):
        if family == socket.AF_UNSPEC:
            raise ValueError('unexpected address family')
        addr_pairs_info = (((family, proto), (None, None)),)
    else:
        # join address by (family, protocol)
        addr_infos = OrderedDict()
        for idx, addr in enumerate((local_addr, remote_addr)):
            if addr is not None:
                assert isinstance(addr, tuple) and len(addr) == 2, (
                        '2-tuple is expected')
                infos = yield event_loop.getaddrinfo(
                        *addr, family=family, type=socket.SOCK_DGRAM,
                        proto=proto, flags=flags)
                if not infos:
                    raise OSError('getaddrinfo() returned empty list')
                for fam, _, pro, _, address in infos:
                    key = (fam, pro)
                    if key not in addr_infos:
                        addr_infos[key] = [None, None]
                    addr_infos[key][idx] = address

        # each addr has to have info for each (family, proto) pair
        addr_pairs_info = [
            (key, addr_pair) for key, addr_pair in addr_infos.items()
            if not ((local_addr and addr_pair[0] is None) or
                    (remote_addr and addr_pair[1] is None))]

        if not addr_pairs_info:
            raise ValueError('can not get address information')

    exceptions = []

    for ((family, proto),
         (local_address, remote_address)) in addr_pairs_info:
        sock = None
        l_addr = None
        r_addr = None
        try:
            sock = socket.socket(
                family=family, type=socket.SOCK_DGRAM, proto=proto)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            if local_addr:
                sock.bind(local_address)
                l_addr = sock.getsockname()
            if remote_addr:
                yield event_loop.sock_connect(sock, remote_address)
                r_addr = remote_address
        except OSError as exc:
            if sock is not None:
                sock.close()
            exceptions.append(exc)
        else:
            break
    else:
        raise exceptions[0]

    protocol = protocol_factory()
    transport = SocketDatagramTransport(
                    event_loop, sock, protocol, r_addr, extra={'addr': l_addr})
    yield transport, protocol