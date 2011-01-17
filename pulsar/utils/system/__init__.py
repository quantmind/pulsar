from .runtime import Platform

platform = Platform()
platformType = platform.getType()
seconds = platform.seconds

if platformType == 'posix':
    from .posixsystem import *
elif platformType == 'win':
    from .windowssystem import *
    
    
def create_socket(conf):
    """Create a new socket for the given address. If the
    address is a tuple, a TCP socket is created. If it
    is a string, a Unix socket is created. Otherwise
    a TypeError is raised.
    """
    # get it only once
    addr = conf.address
    sock_type = create_socket_address(addr)

    if 'PULSAR_FD' in os.environ:
        fd = int(os.environ.pop('PULSAR_FD'))
        try:
            return sock_type(conf, fd=fd)
        except socket.error as e:
            if e[0] == errno.ENOTCONN:
                log.error("PULSAR_FD should refer to an open socket.")
            else:
                raise

    # If we fail to create a socket from GUNICORN_FD
    # we fall through and try and open the socket
    # normally.
    
    for i in range(5):
        try:
            return sock_type(conf)
        except socket.error as e:
            if e[0] == errno.EADDRINUSE:
                log.error("Connection in use: %s" % str(addr))
            if e[0] == errno.EADDRNOTAVAIL:
                log.error("Invalid address: %s" % str(addr))
                sys.exit(1)
            if i < 5:
                log.error("Retrying in 1 second.")
                time.sleep(1)
          
    log.error("Can't connect to %s" % str(addr))
    sys.exit(1)

    