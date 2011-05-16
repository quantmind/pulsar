import os
import socket
from pulsar import system
from .base import BaseSocket, TCPSocket, TCP6Socket, MAXFD


__all__ = ['is_ipv6',
           'get_maxfd',
           'UnixSocket',
           'TCP6Socket',
           'create_socket_address']


    
def get_maxfd():
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
    return maxfd


def is_ipv6(addr):
    try:
        socket.inet_pton(socket.AF_INET6, addr)
    except socket.error: # not a valid address
        return False
    return True    


class UnixSocket(BaseSocket):
    
    FAMILY = socket.AF_UNIX
    
    def __init__(self, conf, fd=None):
        if fd is None:
            try:
                os.remove(conf.address)
            except OSError:
                pass
        super(UnixSocket, self).__init__(conf, fd=fd)
    
    def __str__(self):
        return "unix:%s" % self.address
        
    def bind(self, sock):
        old_umask = os.umask(self.conf.umask)
        sock.bind(self.address)
        system.chown(self.address, self.conf.uid, self.conf.gid)
        os.umask(old_umask)
        
    def close(self):
        super(UnixSocket, self).close()
        os.unlink(self.address)


def create_socket_address(addr):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. If it
    is a string, a Unix socket is created. Otherwise
    a TypeError is raised.
    """
    if isinstance(addr, tuple):
        if is_ipv6(addr[0]):
            sock_type = TCP6Socket
        else:
            sock_type = TCPSocket
    elif isinstance(addr, basestring):
        sock_type = UnixSocket
    else:
        raise TypeError("Unable to create socket from: %r" % addr)

    return sock_type
