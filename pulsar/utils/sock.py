import logging
import sys
import os
import io
import socket
import errno
import time

__all__ = ['Socket',
           'TCPSocket',
           'TCP6Socket',
           'wrap_socket',
           'flush_socket',
           'create_socket',
           'create_connection',
           'create_socket_address',
           'get_maxfd',
           'socket_pair']

logger = logging.getLogger('pulsar.sock')

ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                  errno.EWOULDBLOCK, errno.EPIPE)

MAXFD = 1024


def create_connection(address, blocking = 0):
    sock_type = create_socket_address(address)
    s = sock_type(is_server = False)
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
    """Create a new server :class:`Socket` for the given address.
If the address is a tuple, a TCP socket is created.
If it is a string, a Unix socket is created.
Otherwise a TypeError is raised.

:parameter address: Socket address.
:parameter log: Optional python logger instance.
:parameter backlog: The maximum number of pending connections or ``None``.
    if ``None`` this is a client socket.
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
    '''Wrap a python socket with pulsar :class:`Socket`.'''
    address = sock.getsockname()
    sock_type = create_socket_address(address)
    return sock_type(fd=sock,bound=True,backlog=None)


def socket_pair(backlog = 2048, log = None):
    '''Create a ``127.0.0.1`` (client,server) socket pair on any
available port. The first socket is connected to the second, the server socket,
which is bound to ``127.0.0.1`` at any available port.

:param backlog: number of connection to listen.
:param log: optional python logger.
:rtype: tuple with two instances of :class:`Socket`
'''
    w = socket.socket()
    w.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    
    count = 0
    while 1:
        count += 1
        s = create_socket(('127.0.0.1',0), log = log, backlog = backlog)
        try:
            w.connect(s.name)
            break
        except socket.error as e:
            if e[0] != errno.WSAEADDRINUSE:
                raise
            if count >= 10:
                s.close()
                w.close()
                raise socket.error("Cannot bind socket pairs!")
            s.close()
    
    w = s.__class__(fd = w, bound = True, backlog = None)
    w.setblocking(0)
    return w,s    
    

class Socket(object):
    '''Wrapper class for a python socket. It provides with
higher level tools for creating and reusing sockets already created.'''
    def __init__(self, address=None, backlog=2048, fd=None, bound=False,
                 is_server = None):
        self.backlog = backlog
        self._is_server = is_server if is_server is not None else not bound
        self._init(fd,address,bound)
        
    def _init(self, fd, address, bound):
        if fd is None:
            self._clean()
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        elif hasattr(fd,'fileno'):
            self.sock = fd
            return
        else:
            if hasattr(socket,'fromfd'):
                sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
            else:
                raise ValueError('Cannot create socket from file deascriptor.\
 Not implemented in your system')
        if self.is_server():
            self.sock = self.set_options(sock, address, bound)
        else:
            self.sock = sock
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d['fd'] = d.pop('sock').fileno()
        return d
    
    def __setstate__(self, state):
        fd = state.pop('fd')
        self.__dict__ = state
        self._init(fd,None,True)
    
    def is_server(self):
        return self._is_server
    
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
            
    def async_recv(self, length):
        try:
            return self.sock.recv(length)
        except socket.error as e:
            if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return None
            else:
                raise
    
    @property
    def name(self):
        return self.sock.getsockname()
    
    def __str__(self, name):
        return "<socket %d>" % self.sock.fileno()
    
    def __repr__(self):
        s = str(self)
        v = "<socket %d>" % self.sock.fileno()
        if s != v:
            return '{0} {1}'.format(v,s)
        else:
            return s
    
    def __getattr__(self, name):
        return getattr(self.sock, name)
    
    def fileno(self):
        '''Return the file descriptor of the socket.'''
        return self.sock.fileno()
    
    def set_options(self, sock, address, bound):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if not bound:
            self.bind(sock, address)
        if self.backlog:
            sock.setblocking(0)
            sock.listen(self.backlog)
        return sock
        
    def bind(self, sock, address):
        sock.bind(address)
        
    def close(self, log = None):
        try:
            self.sock.close()
        except socket.error as e:
            if log:
                log.info("Error while closing socket %s" % str(e))
        time.sleep(0.3)
        
    def info(self):
        if self.is_server():
            return 'listening at {0}'.format(self)
        else:
            return 'client at {0}'.format(self)


class TCPSocket(Socket):
    
    FAMILY = socket.AF_INET
    
    def __str__(self):
        return "http://%s:%d" % self.name
    
    def set_options(self, sock, address, bound):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return super(TCPSocket, self).set_options(sock, address, bound)


class TCP6Socket(TCPSocket):

    FAMILY = socket.AF_INET6

    def __str__(self):
        (host, port, fl, sc) = self.name
        return "http://[%s]:%d" % (host, port)


if os.name == 'posix':
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
            
        def bind(self, sock, address):
            old_umask = os.umask(self.conf.umask)
            sock.bind(address)
            system.chown(address, self.conf.uid, self.conf.gid)
            os.umask(old_umask)
            
        def close(self):
            super(UnixSocket, self).close()
            os.unlink(self.name)


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