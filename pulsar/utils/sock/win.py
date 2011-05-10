
from .base import TCP6Socket, TCPSocket, MAXFD

__all__ = ['is_ipv6',
           'get_maxfd',
           'create_socket_address']


def get_maxfd():
    return MAXFD


def is_ipv6(addr):
    return False


def create_socket_address(addr):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. 
    Otherwise a TypeError is raised.
    """
    if isinstance(addr, tuple):
        if is_ipv6(addr[0]):
            sock_type = TCP6Socket
        else:
            sock_type = TCPSocket
    else:
        raise TypeError("Unable to create socket from: %r" % addr)

    return sock_type
