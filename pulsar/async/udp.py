from pulsar.utils.structures import OrderedDict 
from pulsar.utils.internet import (TRY_WRITE_AGAIN, TRY_READ_AGAIN)
from .internet import SocketTransport


class SocketDatagramTransport(SocketTransport):
    
    def __init__(self, event_loop, sock, protocol, address, **kwargs):
        self._address = address
        super(SocketDatagramTransport, self).__init__(
                                event_loop, sock, protocol, **kwargs)
        
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
                infos = yield self.getaddrinfo(
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
                yield self.sock_connect(sock, remote_address)
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