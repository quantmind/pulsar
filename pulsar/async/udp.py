'''UDP protocol clients and servers'''
from pulsar.utils.sockets import *

from .servers import Server 
from .protocols import Protocol


class UDPProtocol(Protocol):
    '''The User Datagram Protocol. With UDP, computer applications can send
messages, in this case referred to as datagrams, to other hosts on an Internet
Protocol (IP) network without prior communications to set up special
transmission channels or data paths.'''
    pass

    
class UDPServer(Server):
    """UDP server class."""
    TYPE = socket.SOCK_DGRAM
    protocol_factory = UDPProtocol
    max_packet_size = 8192
    
    def ready_read(self):
        try:
            if self.closed:
                return
            try:
                data, client_addr = self._sock.recvfrom(self.max_packet_size)
            except socket.error as e:
                if e.args[0] in TRY_READ_AGAIN:
                    return
                raise
            self.create_connection((data, self.sock), address)
        except Exception:
            LOGGER.exception('Could not accept new connection')
        

    def shutdown_connection(self, request):
        # No need to shutdown anything.
        self.close_request(request)

    def close_connection(self, request):
        # No need to close anything.
        pass
