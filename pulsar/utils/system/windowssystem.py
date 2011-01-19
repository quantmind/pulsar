import multiprocessing.reduction
from select import select as _select

from .base import *

if not ispy3k():
    ALL_SIGNALS = "INT TERM"
    
    def fromfd(fd, family, type, proto=0):
        """ fromfd(fd, family, type[, proto]) -> socket object
    
        Create a socket object from a duplicate of the given file
        descriptor.  The remaining arguments are the same as for socket().
        """
        nfd = dup(fd)
        return socket(family, type, proto, nfd)
    
    socket.fromfd = fromfd 
    
    
def select(r, w, e, timeout=None):
    """Win32 select wrapper."""
    if not (r or w):
        # windows select() exits immediately when no sockets
        if timeout is None:
            timeout = 0.01
        else:
            timeout = min(timeout, 0.001)
        sleep(timeout)
        return [], [], []
    # windows doesn't process 'signals' inside select(), so we set a max
    # time or ctrl-c will never be recognized
    if timeout is None or timeout > 0.5:
        timeout = 0.5
    r, w, e = _select(r, w, w, timeout)
    return r, w + e, []
    
    
def chown(path, uid, gid):
    pass


def close_on_exec(fd):
    pass
    
    
def set_non_blocking(fd):
    pass


def get_maxfd():
    return MAXFD


def get_uid(user):
    return None


def get_gid(group):
    return None


def setpgrp():
    pass


def is_ipv6(addr):
    return False


def create_socket_address(addr):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. 
    Otherwise a TypeError is raised.
    """
    # get it only once    
    if isinstance(addr, tuple):
        if is_ipv6(addr[0]):
            sock_type = TCP6Socket
        else:
            sock_type = TCPSocket
    else:
        raise TypeError("Unable to create socket from: %r" % addr)

    return sock_type
