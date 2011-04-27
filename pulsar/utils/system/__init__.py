import errno
from .runtime import Platform

platform = Platform()
seconds = platform.seconds

if platform.type == 'posix':
    from .posixsystem import *
elif platform.type == 'win':
    from .windowssystem import *

    
def create_socket(address, log = None):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. If it
    is a string, a Unix socket is created. Otherwise
    a TypeError is raised.
    """
    sock_type = create_socket_address(address)

    if 'PULSAR_FD' in os.environ:
        fd = int(os.environ.pop('PULSAR_FD'))
        try:
            return sock_type(address, fd=fd)
        except socket.error as e:
            if e[0] == errno.ENOTCONN:
                obj.log.error("PULSAR_FD should refer to an open socket.")
            else:
                raise

    # If we fail to create a socket from PULSAR_FD
    # we fall through and try and open the socket
    # normally.
    
    for i in range(5):
        try:
            return sock_type(address)
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

    