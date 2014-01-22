import sys
import socket

import pulsar
from pulsar.utils.structures import OrderedDict
from pulsar.utils.exceptions import TooManyConsecutiveWrite
from pulsar.utils.internet import (TRY_WRITE_AGAIN, TRY_READ_AGAIN,
                                   ECONNREFUSED, BUFFER_MAX_SIZE,
                                   format_address)

from .futures import coroutine_return, in_loop, NOT_DONE
from .events import EventHandler
from .internet import (SocketTransport, raise_socket_error,
                       raise_write_socket_error)
from .consts import LOG_THRESHOLD_FOR_CONNLOST_WRITES, MAX_CONSECUTIVE_WRITES
from .protocols import asyncio, PulsarProtocol


__all__ = ['DatagramServer', 'DatagramProtocol']


class DatagramProtocol(PulsarProtocol, asyncio.DatagramProtocol):
    '''An ``asyncio.DatagramProtocol`` which derives
    from :class:`.PulsarProtocol`
    '''


class DatagramTransport(SocketTransport):
    '''A :class:`.SocketTransport` for datagram (UDP) sockets
    '''
    def __init__(self, loop, sock, protocol, address=None, **kw):
        self._address = address
        super(DatagramTransport, self).__init__(loop, sock, protocol, **kw)

    def _do_handshake(self):
        self._loop.add_reader(self._sock_fd, self._ready_read)
        if self._protocol is not None:
            self._loop.call_soon(self._protocol.connection_made, self)

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
        is_writing = bool(self._write_buffer)
        if data:
            # Add data to the buffer
            assert isinstance(data, bytes)
            if len(data) > BUFFER_MAX_SIZE:
                for i in range(0, len(data), BUFFER_MAX_SIZE):
                    chunk = data[i:i+BUFFER_MAX_SIZE]
                    self._write_buffer.append((chunk, addr))
            else:
                self._write_buffer.append((data, addr))
        # Try to write only when not waiting for write callbacks
        if not is_writing:
            self._consecutive_writes = 0
            self._ready_sendto()
            if self._write_buffer:    # still writing
                self.logger.debug('adding writer for %s', self)
                self._loop.add_writer(self._sock_fd, self._ready_sendto)
            elif self._closing:
                self._loop.call_soon(self._shutdown)
        else:
            self._consecutive_writes += 1
            if self._consecutive_writes > MAX_CONSECUTIVE_WRITES:
                self.abort(TooManyConsecutiveWrite())

    def _ready_sendto(self):
        # Do the actual sending
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
                    if self._write_continue(e):
                        break
                    if raise_write_socket_error(e):
                        raise
                    else:
                        return self.abort()
        except Exception as e:
            failure = sys.exc_info()
        else:
            if not self._write_buffer:
                self._loop.remove_writer(self._sock_fd)
                if self._closing:
                    self._loop.call_soon(self._shutdown)
            return tot_bytes
        if not self._closing:
            self.abort(failure)

    def _ready_read(self):
        try:
            try:
                chunk, addr = self._sock.recvfrom(self._read_chunk_size)
            except self.SocketError as e:
                if self._read_continue(e):
                    return
                if raise_socket_error(e):
                    raise
                else:
                    chunk = None
            if chunk:
                self._protocol.datagram_received(chunk, addr)
            else:
                self.close()
            return
        except self.SocketError:
            self._protocol.error_received(sys.exc_info())
        except Exception:
            failure = sys.exc_info()
        if failure:
            self.abort(Failure(failure))


class DatagramServer(EventHandler):
    '''An :class:`.EventHandler` for serving UDP sockets.

    .. attribute:: _transports

        A list of :class:`.DatagramTransport`.

        Available once the :meth:`create_endpoint` method has returned.
    '''
    _transports = None
    _started = None

    ONE_TIME_EVENTS = ('start', 'stop')
    MANY_TIMES_EVENTS = ('pre_request', 'post_request')

    def __init__(self, protocol_factory, loop, address=None,
                 name=None, sockets=None, max_requests=None):
        self._loop = loop or get_event_loop() or new_event_loop()
        super(DatagramServer, self).__init__(self._loop)
        self.protocol_factory = protocol_factory
        self._max_requests = max_requests
        self._requests_processed = 0
        self._name = name or self.__class__.__name__
        self._params = {'address': address, 'sockets': sockets}

    @in_loop
    def create_endpoint(self, **kw):
        '''create the server endpoint.

        :return: a :class:`.Deferred` called back when the server is
            serving the socket.'''
        if hasattr(self, '_params'):
            address = self._params['address']
            sockets = self._params['sockets']
            del self._params
            try:
                transports = []
                if sockets:
                    for sock in sockets:
                        proto = self.create_protocol()
                        transport = DatagramTransport(self._loop, sock, proto)
                        transports.append(transport)
                else:
                    transport, _ = yield self._loop.create_datagram_endpoint(
                        self.protocol_factory, local_addr=adress)
                    transports.append(transport)
                self._transports = transports
                self._started = self._loop.time()
                for transport in self._transports:
                    address = transport._sock.getsockname()
                    self.logger.info('%s serving on %s', self._name,
                                     format_address(address))
                self.fire_event('start')
            except Exception:
                self.fire_event('start', sys.exc_info())

    @in_loop
    def close(self):
        '''Stop serving the :attr:`.Server.sockets` and close all
        concurrent connections.
        '''
        if self._transports:
            transports, self._transports = self._transports, None
            for transport in transports:
                transport.close()
            self.fire_event('stop')
        coroutine_return(self)

    def info(self):
        sockets = []
        server = {'pulsar_version': pulsar.__version__,
                  'python_version': sys.version,
                  'uptime_in_seconds': int(self._loop.time() - self._started),
                  'sockets': sockets,
                  'max_requests': self._max_requests}
        clients = {'requests_processed': self._requests_processed}
        if self._transports:
            for transport in self._transports:
                sockets.append({
                    'address': format_address(transport._sock.getsockname())})
        return {'server': server,
                'clients': clients}

    def create_protocol(self):
        '''Override :meth:`Producer.create_protocol`.
        '''
        protocol = self.protocol_factory(producer=self)
        return protocol


def create_datagram_endpoint(loop, protocol_factory, local_addr,
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
                infos = yield loop.getaddrinfo(
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
        r_addr = None
        try:
            sock = socket.socket(
                family=family, type=socket.SOCK_DGRAM, proto=proto)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(False)
            if local_addr:
                sock.bind(local_address)
            if remote_addr:
                yield loop.sock_connect(sock, remote_address)
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
    transport = DatagramTransport(loop, sock, protocol, r_addr)
    coroutine_return((transport, protocol))
