from pulsar.utils.sockets import *

from .protocols import ServerProtocol

__all__ = ['TCPServer']


class TCPServer(ServerProtocol):
    '''An asynchronous  TCP server
    
.. attribute:: timeout

    number of seconds to keep alive an idle client connection
'''
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
                    sock, address = self._sock.accept()
                except socket.error as e:
                    if e.args[0] in (EWOULDBLOCK, EAGAIN):
                        self.numberAccepts = i
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
                protocol = self.protocol(address)
                if protocol is None:
                    sock.close()
                    continue
                self.received += 1
                session = self.received
                transport = self.transport(self._event_loop, sock, protocol,
                                           server=self, session=self.received)
                protocol.connection_made(transport)
        except:
            LOGGER.error('Could not accept new connection', exc_info=True)
            
    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except:
            pass
        