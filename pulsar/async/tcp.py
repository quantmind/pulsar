'''Protocols for TCP clients and servers'''
import logging

from pulsar.utils.sockets import *
from pulsar.utils.structures import merge_prefix

from .protocols import ServerProtocol, ClientProtocol

__all__ = ['TCPServer', 'TCPClient']

TRAY_AGAIN = (EWOULDBLOCK, ENOBUFS, EINPROGRESS)

LOGGER = logging.getLogger('pulsar.tcp')

class TCPClient(ClientProtocol):
    
    def connect(self, sock):
        try:
            sock.connect(self.address)
        except socket.error as e:
            if e.args[0] in TRY_AGAIN:
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
                if e.args[0] in TRY_AGAIN:
                    break
                else:
                    raise
        return tot_bytes


class TCPServer(ServerProtocol):
    '''An asynchronous  TCP server
    
.. attribute:: timeout

    number of seconds to keep alive an idle client connection
'''
    protocol = TCPClient
    def __init__(self, numberAccepts=100, transport=None, response=None,
                 timeout=30, max_requests=0, protocol=None, **params):
        if platform.type == "posix":
            self._numberAccepts = max(numberAccepts, 1)
        else:
            self._numberAccepts = 1
        self.timeout = timeout
        self.max_requests = max_requests
        if protocol:
            self.protocol = protocol
        if response:
            self.response = response 
        
    def ready_read(self):
        try:
            for i in range(self._numberAccepts):
                if self.closed:
                    return
                try:
                    sock, address = self.sock.accept()
                except socket.error as e:
                    if e.args[0] in (EWOULDBLOCK, EAGAIN):
                        break
                    elif e.args[0] == EPERM:
                        # Netfilter on Linux may have rejected the
                        # connection, but we get told to try to accept() anyway.
                        continue
                    elif e.args[0] in TCP_ACCEPT_ERRORS:
                        LOGGER.info('Could not accept new connection')
                        break
                    raise
                # Build the protocol
                protocol = self.protocol(address, self.response)
                if protocol is None:
                    sock.close()
                    continue
                self.received += 1
                self.transport(sock, protocol, session=self.received)
        except:
            LOGGER.exception('Could not accept new connection')
        
