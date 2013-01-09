from pulsar.utils.sockets import *

from .transports import ServerTransport

__all__ = ['TCPServer']


class TCPServer(ServerTransport):
    '''An asynchronous  TCP server
    
.. attribute:: timeout

    number of seconds to keep alive an idle client connection
'''
    def setup(self, numberAccepts=100, transport=None, response=None,
              timeout=30, max_requests=0, **params):
        if platform.type == "posix":
            self._numberAccepts = max(numberAccepts, 1)
        else:
            self._numberAccepts = 1
        self.timeout = timeout
        self.max_requests = max_requests
        if transport:
            self.transport = transport
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
            
    def on_close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except:
            pass
        self._sock = None
        