import logging
import sys
import os
import io
import socket
import errno
import time

__all__ = ['IStream',
           'BaseSocket',
           'Socket',
           'TCPSocket',
           'TCP6Socket',
           'wrap_socket',
           'get_socket_timeout',
           'flush_socket',
           'create_socket',
           'create_client_socket',
           'server_client_sockets',
           'create_connection',
           'create_socket_address',
           'get_maxfd',
           'socket_pair',
           'server_socket']

logger = logging.getLogger('pulsar.sock')

ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                  errno.EWOULDBLOCK, errno.EPIPE)

MAXFD = 1024

def get_socket_timeout(val):
    '''Obtain a valid stocket timeout value from *val*'''
    if val is None:
        return val  # blocking socket
    else:
        val = float(val)
    if val < 0:
        return None # Negative values for blocking sockets
    else:
        ival = int(val)
        return ival if ival == val else val

def create_connection(address, blocking=0):
    sock_type, address = create_socket_address(address)
    s = sock_type(is_server=False)
    s.sock.connect(address)
    s.sock.setblocking(blocking)
    return s

def flush_socket(sock, length=None):
    length = length or io.DEFAULT_BUFFER_SIZE
    client, addr = sock.accept()
    if client:
        r = client.recv(length)
        while len(r) > length:
            r = client.recv(length)

def create_socket(address, log=None, backlog=2048,
                  bound=False, retry=5, retry_lag=2):
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
    sock_type, address = create_socket_address(address)
    for i in range(retry):
        try:
            return sock_type(address, backlog=backlog, bound=bound)
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                if log:
                    log.error("Connection in use: %s" % str(address))
            elif e.errno == errno.EADDRNOTAVAIL:
                if log:
                    log.error("Invalid address: %s" % str(address))
                raise RuntimeError("Invalid address: %s" % str(address))
            if i < retry:
                if log:
                    log.error("Retrying in {0} seconds.".format(retry_lag))
                time.sleep(retry_lag)
    raise RuntimeError("Can't connect to %s. Tried %s times." %
                       (str(address), retry))


create_client_socket = lambda address: create_socket(address, bound=True)

def wrap_socket(sock):
    '''Wrap a python socket with pulsar :class:`Socket`.'''
    if sock and not isinstance(sock, Socket):
        address = sock.getsockname()
        sock_type, address = create_socket_address(address)
        return sock_type(fd=sock,bound=True,backlog=None)
    else:
        return sock

def socket_pair(backlog=2048, log=None, blocking=0):
    '''Create a ``127.0.0.1`` (client,server) socket pair on any
available port. The first socket is connected to the second, the server socket,
which is bound to ``127.0.0.1`` at any available port.

:param backlog: number of connection to listen.
:param log: optional python logger.
:rtype: tuple with two instances of :class:`Socket`
'''
    remote_client = socket.socket()
    remote_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    count = 0
    while 1:
        count += 1
        server = create_socket(('127.0.0.1',0), log=log, backlog=backlog)
        try:
            # Connect the remote socket with the server socket
            remote_client.connect(server.name)
            break
        except socket.error as e:
            if e[0] != errno.WSAEADDRINUSE:
                raise
            if count >= 10:
                remote_client.close()
                server.close()
                raise socket.error("Cannot bind socket pairs!")
            remote_client.close()

    remote_client = server.__class__(fd=remote_client, bound=True, backlog=None)
    remote_client.setblocking(blocking)
    return remote_client, server

def server_client_sockets(backlog=2048, blocking=0):
    '''Create a server_connection, client pair.'''
    # get a socket pair
    client, server = socket_pair(backlog=backlog)
    server.setblocking(True)
    server_connection, _ = server.accept()
    server_connection.setblocking(blocking)
    client.setblocking(blocking)
    server_connection = server.__class__(fd=server_connection,
                                         bound=True, backlog=None)
    return server_connection, client

def server_socket(backlog=2048, blocking=0):
    '''Create a TCP socket ready for accepting connections.'''
    # get a socket pair
    w, s = socket_pair(backlog=backlog)
    s.setblocking(True)
    r, _ = s.accept()
    r.close()
    w.close()
    s.setblocking(blocking)
    return s


class IStream(object):
    '''Interface for all streams.

.. attribute: address

    The address of the stream. This is usually the address of the listening
    or writing socket.

.. attribute: log

    Logger instance
'''
    address = ('0.0.0.0', 0)
    log = logging.getLogger('pulsar.IStream')

    def close(self, msg=None):
        '''Close the stream'''
        pass

    def fileno(self):
        '''Return the file descriptor of the socket.'''
        pass

    def write(self, data):
        '''Write *data* into the stream'''
        raise NotImplementedError()


class BaseSocket(IStream):
    sock = None
    
    def getsockname(self):
        try:
            return self.sock.getsockname()
        except socket.error as e:
            # In windows the function raises an exception if the socket
            # is not connected
            if os.name == 'nt' and e.args[0] == errno.WSAEINVAL:
                return ('0.0.0.0', 0)
            else:
                raise
    
    def settimeout(self, value):
        self.sock.settimeout(value)
        
    def gettimeout(self):
        if self.sock:
            return self.sock.gettimeout()
    
    @property
    def async(self):
        return self.gettimeout() == 0
    
    @property
    def address(self):
        return self.getsockname()
    
    def fileno(self):
        if self.sock:
            return self.sock.fileno()
    
    
class Socket(BaseSocket):
    '''Wrapper class for a python socket. It provides with
higher level tools for creating and reusing sockets already created.'''
    def __init__(self, address=None, backlog=2048, fd=None, bound=False,
                 is_server=None):
        self.backlog = backlog
        self._is_server = is_server if is_server is not None else not bound
        self._init(fd, address, bound)

    def _init(self, fd, address, bound):
        if fd is None:
            self._clean()
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        elif hasattr(fd, 'fileno'):
            self.sock = fd
            return
        else:
            if hasattr(socket, 'fromfd'):
                sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
            else:
                raise ValueError('Cannot create socket from file deascriptor.\
 Not implemented in your system')
        if self.is_server:
            self.sock = self.set_options(sock, address, bound)
        else:
            self.sock = sock

    @property
    def closed(self):
        return self.sock == None

    def __getstate__(self):
        d = self.__dict__.copy()
        d['fd'] = d.pop('sock').fileno()
        return d

    def __setstate__(self, state):
        fd = state.pop('fd')
        self.__dict__ = state
        self._init(fd, None, True)

    @property
    def is_server(self):
        return self._is_server

    def _clean(self):
        pass

    def write(self, data):
        '''Same as the socket send method but it close the connection if
not data was sent. In this case it also raises a socket error.'''
        if self.closed or not data:
            return 0
        try:
            sent = self.send(data)
            if sent == 0:
                raise socket.error()
            return sent
        except:
            self.close()
            raise

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

    def recv(self, length=None):
        return self.sock.recv(length or io.DEFAULT_BUFFER_SIZE)

    @property
    def name(self):
        return self.address

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

    def set_options(self, sock, address, bound):
        '''Options for a server socket'''
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # If the socket is not bound, bind it to the address
        if not bound:
            self.bind(sock, address)
        if self.backlog:
            sock.setblocking(0)
            sock.listen(self.backlog)
        return sock

    def bind(self, sock, address):
        sock.bind(address)

    def close(self, log=None):
        '''Shutdown and close the socket.'''
        if not self.closed:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
                self.sock.close()
            except socket.error:
                pass
            self.sock = None

    def info(self):
        if self.is_server:
            return 'listening at {0}'.format(self)
        else:
            return 'client at {0}'.format(self)


class TCPSocket(Socket):
    FAMILY = socket.AF_INET

    def __str__(self):
        return "%s:%d" % self.name

    def set_options(self, sock, address, bound):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return super(TCPSocket, self).set_options(sock, address, bound)


class TCP6Socket(TCPSocket):
    FAMILY = socket.AF_INET6

    def __str__(self):
        (host, port, fl, sc) = self.name
        return "[%s]:%d" % (host, port)


def create_tcp_socket_address(addr):
    if not isinstance(addr, tuple):
        hp = addr.split(':')
        if len(hp) == 2:
            addr = tuple(hp)
    if isinstance(addr, tuple):
        if len(addr) != 2:
            raise ValueError('TCP address must be a (host, port) tuple')
        host = addr[0]
        try:
            port = int(addr[1])
        except:
            raise ValueError('TCP address must be a (host, port) tuple')
        if not host:
            host = '0.0.0.0'
        addr = (host, port)
        if is_ipv6(host):
            sock_type = TCP6Socket
        else:
            sock_type = TCPSocket
    else:
        raise TypeError("Unable to create socket from: %r" % addr)
    return sock_type, addr

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
        try:
            return create_tcp_socket_address(addr)
        except TypeError:
            if isinstance(addr, str):
                sock_type = UnixSocket
            else:
                raise TypeError("Unable to create socket from: %r" % addr)

        return sock_type, addr

else:
    def get_maxfd():
        return MAXFD


    def is_ipv6(addr):
        return False

    create_socket_address = create_tcp_socket_address

