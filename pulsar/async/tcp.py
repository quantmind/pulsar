'''TCP protocol clients and servers'''
import logging

from pulsar.utils.sockets import *
from pulsar.utils.structures import merge_prefix

from .servers import Server
from .protocols import Protocol

__all__ = ['TCPServer', 'TCPProtocol']

TRY_WRITE_AGAIN = (EWOULDBLOCK, ENOBUFS, EINPROGRESS)
TRY_READ_AGAIN = (EWOULDBLOCK, EAGAIN)

LOGGER = logging.getLogger('pulsar.tcp')

class TCPProtocol(Protocol):
    '''TCP protocol.'''
    def connect(self, sock):
        try:
            sock.connect(self.address)
        except socket.error as e:
            if e.args[0] in TRY_WRITE_AGAIN:
                return False
            else:
                raise
        else:
            return True #    A synchronous connection
    
    def ready_write(self):
        buffer = self._transport._write_buffer
        tot_bytes = 0
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


class TCPServer(Server):
    '''An asynchronous TCP :class:`Server`'''
    TYPE = socket.SOCK_STREAM
    protocol_factory = TCPProtocol
    
    def __init__(self, event_loop, sock, numberAccepts=100, **params):
        if platform.type == "posix":
            self._numberAccepts = max(numberAccepts, 1)
        else:
            self._numberAccepts = 1
        super(TCPServer, self).__init__(event_loop, sock, **params)
        
    def ready_read(self):
        try:
            for i in range(self._numberAccepts):
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
                self.create_connection(sock, address)
        except:
            LOGGER.exception('Could not accept new connection')
        
