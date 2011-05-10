import sys
import errno
import time
import socket

from .plat import *

__all__ = ['create_socket']
    

def create_socket(address, log = None, bound = False):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. If it
    is a string, a Unix socket is created. Otherwise
    a TypeError is raised.
    """
    sock_type = create_socket_address(address)
    
    for i in range(5):
        try:
            return sock_type(address, log = log, bound = bound)
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                if log:
                    log.error("Connection in use: %s" % str(address))
            elif e.errno == errno.EADDRNOTAVAIL:
                if log:
                    log.error("Invalid address: %s" % str(address))
                sys.exit(1)
            if i < 5:
                if log:
                    log.error("Retrying in 1 second.")
                time.sleep(1)
    
    if log:
        log.error("Can't connect to %s" % str(address))
    sys.exit(1)
