import logging
import sys
import socket
import errno
import time

from .system import platform

__all__ = ['Socket',
           'TCPSocket',
           'TCP6Socket',
           'wrap_socket',
           'flush_socket',
           'create_socket',
           'create_connection',
           'create_socket_address',
           'get_maxfd']

logger = logging.getLogger('pulsar.sock')

ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                  errno.EWOULDBLOCK, errno.EPIPE)

MAXFD = 1024


def create_connection(address, blocking = False):
    sock_type = create_socket_address(address)
    s = sock_type(address, backlog = None, bound = True)
    s.sock.connect(address)
    s.sock.setblocking(blocking)
    return s
    

def flush_socket(sock):
    client, addr = sock.accept()
    if client:
        r = client.recv(1024)
        while len(r) > 1024:
            r = client.recv(1024)
    

def create_socket(address, log = None, backlog = 2048,
                  bound = False, retry = 5, retry_lag = 2):
    """Create a new :class:`Socket` for the given address.
If the address is a tuple, a TCP socket is created.
If it is a string, a Unix socket is created.
Otherwise a TypeError is raised.

:parameter address: Socket address.
:parameter log: Optional python logger instance.
:parameter backlog: The maximum number of pending connections.
:parameter bound: If ``False`` the socket will bind to *address* otherwise
    it is assumed to be already bound.
:parameter retry: Number of retries before aborting.
:parameter retry_lag: Number of seconds between connection attempts.
:rtype: Instance of :class:`Socket`
    """
    sock_type = create_socket_address(address)
    
    for i in range(retry):
        try:
            return sock_type(address, backlog = backlog, bound = bound)
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                if log:
                    log.error("Connection in use: %s" % str(address))
            elif e.errno == errno.EADDRNOTAVAIL:
                if log:
                    log.error("Invalid address: %s" % str(address))
                sys.exit(1)
            if i < retry:
                if log:
                    log.error("Retrying in {0} seconds.".format(retry_lag))
                time.sleep(retry_lag)
    
    if log:
        log.error("Can't connect to %s" % str(address))
    sys.exit(1)
    

def wrap_socket(sock):
    address = sock.getsockname()
    sock_type = create_socket_address(address)
    return sock_type(address,fd=sock,bound=True)    
    

class Socket(object):
    '''Wrapper class for a python socket. It provides with
higher level tools for creating and reusing sockets already created.'''
    def __init__(self, address, backlog=2048, fd=None, bound=False):
        self.address = address
        self.backlog = backlog
        self._init(fd,bound)
        
    def _init(self, fd, bound = False):
        if fd is None:
            self._clean()
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        elif hasattr(fd,'fileno'):
            sock = fd
        else:
            if hasattr(socket,'fromfd'):
                sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
            else:
                raise ValueError('Cannot create socket from file deascriptor.\
 Not implemented in your system')
        self.sock = self.set_options(sock, bound=bound)
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d['fd'] = d.pop('sock').fileno()
        return d
    
    def __setstate__(self, state):
        fd = state.pop('fd')
        self.__dict__ = state
        self._init(fd,True)
    
    def _clean(self):
        pass
    
    def accept(self):
        '''Wrap the socket accept method.'''
        client = None
        try:
            return self.sock.accept()
        except socket.error as e:
            if e.errno not in ALLOWED_ERRORS:
                raise
            else:
                return None,None
    
    @property
    def name(self):
        return self.sock.getsockname()
    
    def __str__(self, name):
        return "<socket %d>" % self.sock.fileno()
    
    def __getattr__(self, name):
        return getattr(self.sock, name)
    
    def fileno(self):
        '''Return the file descriptor of the socket.'''
        return self.sock.fileno()
    
    def set_options(self, sock, bound=False):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not bound:
            self.bind(sock)
        if self.backlog:
            sock.setblocking(0)
            sock.listen(self.backlog)
        return sock
        
    def bind(self, sock):
        sock.bind(self.address)
        
    def close(self, log = None):
        try:
            self.sock.close()
        except socket.error as e:
            if log:
                log.info("Error while closing socket %s" % str(e))
        time.sleep(0.3)
        del self.sock


class TCPSocket(Socket):
    
    FAMILY = socket.AF_INET
    
    def __str__(self):
        return "http://%s:%d" % self.name
    
    def set_options(self, sock, bound=False):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return super(TCPSocket, self).set_options(sock, bound=bound)


class TCP6Socket(TCPSocket):

    FAMILY = socket.AF_INET6

    def __str__(self):
        (host, port, fl, sc) = self.name
        return "http://[%s]:%d" % (host, port)


if platform.type == 'posix':
    import resource
    
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


    class UnixSocket(Socket):
        
        FAMILY = socket.AF_UNIX
        
        def _clean(self):
            try:
                os.remove(conf.address)
            except OSError:
                pass
        
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
        elif isinstance(addr, str):
            sock_type = UnixSocket
        else:
            raise TypeError("Unable to create socket from: %r" % addr)
    
        return sock_type
    
else:
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