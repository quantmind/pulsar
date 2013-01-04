import logging
import sys
import os
import io
import socket
import errno
import time

from .httpurl import native_str

__all__ = ['IStream',
           'BaseSocket',
           'Socket',
           'TCPSocket',
           'TCP6Socket',
           'wrap_socket',
           'parse_address',
           'get_socket_timeout',
           'create_socket',
           'create_client_socket',
           'server_client_sockets',
           'create_connection',
           'create_socket_address',
           'socket_pair',
           'server_socket']

LOGGER = logging.getLogger('pulsar.sock')

ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                  errno.EWOULDBLOCK, errno.EPIPE,
                  errno.EINVAL)

MAXFD = 1024

def parse_address(netloc, default_port=8000):
    '''Parse an address and return a tuple with host and port'''
    netloc = native_str(netloc)
    if netloc.startswith("unix:"):
        return netloc.split("unix:")[1]
    # get host
    if '[' in netloc and ']' in netloc:
        host = netloc.split(']')[0][1:].lower()
    elif ':' in netloc:
        host = netloc.split(':')[0].lower()
    elif netloc == "":
        host = "0.0.0.0"
    else:
        host = netloc.lower()
    #get port
    netloc = netloc.split(']')[-1]
    if ":" in netloc:
        port = netloc.split(':', 1)[1]
        if not port.isdigit():
            raise RuntimeError("%r is not a valid port number." % port)
        port = int(port)
    else:
        port = default_port 
    return (host, port)

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

def create_socket(address, logger=None, backlog=2048,
                  bound=False, retry=5, retry_lag=2,
                  retry_step=2, interval_max=30):
    """Create a new server :class:`Socket` for the given address.
If the address is a tuple, a TCP socket is created.
If it is a string, a Unix socket is created.
Otherwise a TypeError is raised.

:parameter address: Socket address.
:parameter logger: Optional python logger instance.
:parameter backlog: The maximum number of pending connections or ``None``.
    if ``None`` this is a client socket.
:parameter bound: If ``False`` the socket will bind to *address* otherwise
    it is assumed to be already bound.
:parameter retry: Number of retries before aborting.
:parameter retry_lag: Number of seconds between connection attempts.
:rtype: Instance of :class:`Socket`
    """
    sock_type, address = create_socket_address(address)
    logger = logger or LOGGER
    lag = retry_lag
    for i in range(retry):
        try:
            return sock_type(address, backlog=backlog, bound=bound)
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                logger.error("Connection in use: %s", address)
            elif e.errno in (errno.EADDRNOTAVAIL, 11004, -5):
                raise RuntimeError("Invalid address: %s" % str(address))
            if i < retry:
                logger.error("Retrying in %s seconds.", lag)
                time.sleep(lag)
                lag = min(lag + retry_step, interval_max)
    raise RuntimeError("Can't connect to %s. Tried %s times." %
                       (str(address), retry))


create_client_socket = lambda address: create_socket(address, bound=True)

def wrap_socket(sock):
    '''Wrap a python socket with pulsar :class:`Socket`.'''
    if sock and not isinstance(sock, Socket):
        address = sock.getsockname()
        sock_type, address = create_socket_address(address)
        return sock_type(fd=sock, bound=True, backlog=None)
    else:
        return sock

def socket_pair(backlog=2048, logger=None, blocking=0):
    '''Create a ``127.0.0.1`` (client,server) socket pair on any
available port. The first socket is connected to the second, the server socket,
which is bound to ``127.0.0.1`` at any available port.

:param backlog: number of connection to listen.
:param logger: optional python logger.
:rtype: tuple with two instances of :class:`Socket`
'''
    remote_client = socket.socket()
    remote_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    count = 0
    while 1:
        count += 1
        server = create_socket(('127.0.0.1',0), logger=logger, backlog=backlog)
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
    or writing socket.'''
    address = ('0.0.0.0', 0)

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
    '''Base class for :class:`IStream` using a socket as I/O mechanism.
    
.. attribute:: sock
    
    The underlying socket
    
.. attribute:: async
    
    True if this is an asynchronous socket.
    
.. attribute:: address
    
    same as :meth:`getsockname`.
'''
    sock = None
    
    def getsockname(self):
        '''The socket name if open otherwise None.'''
        try:
            return self.sock.getsockname()
        except:
            return None
    
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
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        elif hasattr(fd, 'fileno'):
            self.sock = fd
            return
        else:
            sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
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
        try:
            return self.sock.accept()
        except socket.error as e:
            if e.errno not in ALLOWED_ERRORS:
                raise
            else:
                return None, None

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
        return s if s == v else '%s %s' % (v, s)

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
            except:
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
        name = self.name
        if name:
            return "%s:%d" % name
        else:
            return '<closed>'

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

    def is_ipv6(addr):
        try:
            socket.inet_pton(socket.AF_INET6, addr)
        except socket.error: # not a valid address
            return False
        return True


    class UnixSocket(Socket):

        FAMILY = socket.AF_UNIX

        def __str__(self):
            return "unix:%s" % self.address

        def bind(self, sock, address):
            try:
                os.remove(address)
            except OSError:
                pass
            #old_umask = os.umask(self.conf.umask)
            sock.bind(address)
            #system.chown(address, self.conf.uid, self.conf.gid)
            #os.umask(old_umask)

        def close(self):
            name = self.name
            super(UnixSocket, self).close()
            if name:
                try:
                    os.remove(name)
                except OSError:
                    pass


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

else:   #pragma    nocover

    def is_ipv6(addr):
        return False

    create_socket_address = create_tcp_socket_address

