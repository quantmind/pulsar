import os
import sys
import socket

from .system import platform
from .httpurl import native_str


WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb
SOCKET_TYPES = {}

class SocketType(type):
    
    def __new__(cls, name, bases, attrs):
        new_class = super(SocketType, cls).__new__(cls, name, bases, attrs)
        family = getattr(new_class, 'FAMILY', None)
        if family is not None:
            SOCKET_TYPES[family] = new_class
        return new_class


class Socket(SocketType('SocketBase', (), {})):
    '''Wrapper for a socket'''
    def __init__(self, sock, address=None, bindto=False, backlog=1024):
        if sock is None:
            sock = socket.socket(self.FAMILY, socket.SOCK_STREAM)
        self._sock = sock
        self._backlog = backlog if bindto else None
        self._set_options(bindto, address)
    
    @property
    def address(self):
        if self._sock:
            return self._sock.getsockname()
    
    def __str__(self):
        return self.__repr__()
    
    def __getstate__(self):
        d = self.__dict__.copy()
        d['fd'] = d.pop('sock').fileno()
        return d

    def __setstate__(self, state):
        fd = state.pop('fd')
        self.__dict__ = state
        self._sock = socket.fromfd(fd, self.FAMILY, socket.SOCK_STREAM)
        self._set_options()
        
    def _set_options(self, bindto=False, address=None):
        '''Options for a server socket'''
        sock = self._sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # If the socket is not bound, bind it to the address
        if bindto and address:
            self.bind(address)
        if self._backlog:
            sock.setblocking(0)
            sock.listen(self._backlog)
    
    def bind(self, address):
        self._sock.bind(address)
        
    def close(self, log=None):
        '''Shutdown and close the socket.'''
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
                self._sock.close()
            except:
                pass
            self._sock = None
        
    def __getattr__(self, name):
        return getattr(self._sock, name)
        
        
if platform.isWindows:    #pragma    nocover
    EPERM = object()
    from errno import WSAEINVAL as EINVAL
    from errno import WSAEWOULDBLOCK as EWOULDBLOCK
    from errno import WSAEINPROGRESS as EINPROGRESS
    from errno import WSAEALREADY as EALREADY
    from errno import WSAECONNRESET as ECONNRESET
    from errno import WSAEISCONN as EISCONN
    from errno import WSAENOTCONN as ENOTCONN
    from errno import WSAEINTR as EINTR
    from errno import WSAENOBUFS as ENOBUFS
    from errno import WSAEMFILE as EMFILE
    from errno import WSAECONNRESET as ECONNABORTED
    # No such thing as WSAENFILE, either.
    ENFILE = object()
    # Nor ENOMEM
    ENOMEM = object()
    EAGAIN = EWOULDBLOCK
else:
    from errno import EPERM, EINVAL, EWOULDBLOCK, EINPROGRESS, EALREADY,\
                      ECONNRESET, EISCONN, ENOTCONN, EINTR, ENOBUFS, EMFILE,\
                      ENFILE, ENOMEM, EAGAIN, ECONNABORTED
    
    class UnixSocket(Socket):
        FAMILY = socket.AF_UNIX
        
        def __repr__(self):
            return "unix:%s" % self.address
        
        @property
        def type(self):
            return 'unix'
    
        def bind(self, address):
            try:
                os.remove(address)
            except OSError:
                pass
            #old_umask = os.umask(self.conf.umask)
            self._sock.bind(address)
            #system.chown(address, self.conf.uid, self.conf.gid)
            #os.umask(old_umask)

        def close(self):
            address = self.address
            super(UnixSocket, self).close()
            if address:
                try:
                    os.remove(address)
                except OSError:
                    pass

TCP_ACCEPT_ERRORS = (EMFILE, ENOBUFS, ENFILE, ENOMEM, ECONNABORTED)

        
class TCPSocket(Socket):
    FAMILY = socket.AF_INET
    
    @property
    def type(self):
        return 'tcp'
        
    def __repr__(self):
        address = self.address
        if address:
            return '%s:%s' % address
        else:
            return '%s:closed' % self.type
        
class TCP6Socket(TCPSocket):
    FAMILY = socket.AF_INET6
    
    @property
    def type(self):
        return 'tcp6'
    
    def __repr__(self):
        address = self.address
        if address:
            return '[%s]:%s' % address[:2]
        else:
            return '%s:closed' % self.type
        
    def _set_options(self, bindto=False, address=None):
        super(TCP6Socket, self)._set_options(bindto, address)
        if platform.type == "posix" and sys.platform != "cygwin":
            # Required: Forces listenTCP6 to listen exclusively on IPv6 addresses.
            # See: http://www.velocityreviews.com/forums/t328345-ipv6-question.html
            self._sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    
    
def is_ipv6(address):
    '''Determine whether the given string represents an IPv6 address'''
    if '%' in address:
        address = address.split('%', 1)[0]
    if not address:
        return False
    try:
        socket.inet_pton(socket.AF_INET6, address)
    except (ValueError, socket.error):
        return False
    return True

def parse_address(netloc, default_port=8000):
    '''Parse an address and return a tuple with host and port'''
    if isinstance(netloc, tuple):
        return netloc
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
        
def create_socket(address=None, sock=None, bindto=False, backlog=1024):
    if isinstance(sock, Socket):
        return sock
    if sock is None:
        address = parse_address(address)
    else:
        address = sock.getsockname()
    if isinstance(address, tuple):
        return TCPSocket(sock, address, bindto=bindto, backlog=backlog)
    elif is_ipv6(address):
        return TCP6Socket(sock, address, bindto=bindto, backlog=backlog)
    elif platform.type == 'posix':
        return UnixSocket(sock, address, bindto=bindto, backlog=backlog)
    else:
        raise RuntimeError('Socket address not supported in this platform')
    
    
def wrap_client_socket(sock, timeout=0):
    '''Wrap a python socket with pulsar :class:`Socket`.'''
    if sock and not isinstance(sock, Socket):
        sock = SOCKET_TYPES[sock.family](sock)
        sock.settimeout(timeout)
    return sock