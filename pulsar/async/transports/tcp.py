'''TCP protocol clients and servers'''
import socket

from pulsar.utils.sockets import *
from pulsar.utils.pep import range
from pulsar.utils.structures import merge_prefix

from .servers import Server
from .transport import SocketTransport, LOGGER

__all__ = ['TCP']

TRY_WRITE_AGAIN = (EWOULDBLOCK, ENOBUFS, EINPROGRESS)
TRY_READ_AGAIN = (EWOULDBLOCK, EAGAIN)
NUMBER_ACCEPTS = 30 if platform.type == "posix" else 1


class TCP(SocketTransport):
    '''Transport for the TCP protocol.'''
    TYPE = socket.SOCK_STREAM
    
    def _protocol_connect(self, address):
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