'''UDP protocol clients and servers'''
import logging
import socket

from pulsar.utils.sockets import *
 
from .transport import SocketTransport, LOGGER

__all__ = ['UDP']


TRY_READ_AGAIN = (EINTR, EWOULDBLOCK, EMSGSIZE, EINPROGRESS)
ERROR_READ = (ECONNREFUSED, ECONNRESET, ENETRESET, ETIMEDOUT)


class UDP(SocketTransport):
    '''The User Datagram Protocol. With UDP, computer applications can send
messages, in this case referred to as datagrams, to other hosts on an Internet
Protocol (IP) network without prior communications to set up special
transmission channels or data paths.'''
    TYPE = socket.SOCK_DGRAM
    max_packet_size = 8192
    max_throughput = 256 * 1024 # max bytes we read in one eventloop iteration
    
    def _protocol_accept(self):
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
        
    def _protocol_read(self):
        while read < self.max_throughput:
            try:
                data, addr = self.socket.recvfrom(self.max_packet_size)
            except socket.error as se:
                no = se.args[0]
                if no in TRY_READ_AGAIN:
                    return
                #if no in ERROR_READ:
                #    if self._connectedAddr:
                #        self.protocol.connectionRefused()
                #    return
                raise
            else:
                read += len(data)
                self.datagram_received(data, addr)
    _protocol_accept = _protocol_read
    