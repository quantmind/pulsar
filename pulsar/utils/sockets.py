'''
Collection of utilities for sockets and address parsing.

Parse a connection string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_connection_string

'''
import os
import sys
import socket

try:
    from select import poll, POLLIN
except ImportError: #pragma    nocover
    poll = False
    try:
        from select import select
    except ImportError: #pragma    nocover
        select = False
        
from .system import platform
from .pep import native_str
from .httpurl import urlsplit, parse_qsl, urlencode


WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb


class TransportInfo:
    
    def __init__(self, type):
        self.type = type
        self.family = {}
        self.transport = None
        

class TransportType(type):
    TRANSPORT_TYPES = {}
        
    def __new__(cls, name, bases, attrs):
        new_class = super(TransportType, cls).__new__(cls, name, bases, attrs)
        type = getattr(new_class, 'TYPE', None)
        if type is not None:
            if type  not in cls.TRANSPORT_TYPES:
                cls.TRANSPORT_TYPES[type] = TransportInfo(type)
        return new_class
    
    @classmethod
    def get_transport_type(cls, type):
        return cls.TRANSPORT_TYPES[type]
    

get_transport_type = TransportType.get_transport_type
    
    
class SocketType(TransportType):
    
    def __new__(cls, name, bases, attrs):
        new_class = super(SocketType, cls).__new__(cls, name, bases, attrs)
        family = getattr(new_class, 'FAMILY', None)
        type = getattr(new_class, 'TYPE', None)
        if type is not None:
            transport_type = cls.TRANSPORT_TYPES[type]
            if family is not None:
                transport_type.family[family] = new_class
        return new_class


class Socket(SocketType('SocketBase', (), {})):
    '''Wrapper for a socket'''
    def __init__(self, sock=None, address=None, bindto=False, backlog=1024):
        if sock is None:
            sock = socket.socket(self.FAMILY, self.TYPE)
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
        d['fd'] = d.pop('_sock').fileno()
        return d

    def __setstate__(self, state):
        fd = state.pop('fd')
        self.__dict__ = state
        self._sock = socket.fromfd(fd, self.FAMILY, self.TYPE)
        self._set_options()
        
    def _set_options(self, bindto=False, address=None):
        '''Options for a server socket'''
        sock = self._sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # If the socket is not bound, bind it to the address
        if bindto and address:
            self.bind(address)
        if self._backlog is not None:
            sock.setblocking(0)
            sock.listen(self._backlog)
    
    def bind(self, address):
        self._sock.bind(address)
        
    def close(self, log=None):
        '''Shutdown and close the socket.'''
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        
    def __getattr__(self, name):
        return getattr(self._sock, name)
        
        
if platform.is_windows:    #pragma    nocover
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
    from errno import WSAEADDRINUSE as EADDRINUSE
    from errno import WSAEMSGSIZE as EMSGSIZE
    from errno import WSAENETRESET as ENETRESET
    from errno import WSAETIMEDOUT as ETIMEDOUT
    from errno import WSAECONNREFUSED as ECONNREFUSED
    # No such thing as WSAENFILE, either.
    ENFILE = object()
    # Nor ENOMEM
    ENOMEM = object()
    EAGAIN = EWOULDBLOCK
else:
    from errno import EPERM, EINVAL, EWOULDBLOCK, EINPROGRESS, EALREADY,\
                      ECONNRESET, EISCONN, ENOTCONN, EINTR, ENOBUFS, EMFILE,\
                      ENFILE, ENOMEM, EAGAIN, ECONNABORTED, EADDRINUSE,\
                      EMSGSIZE, ENETRESET, ETIMEDOUT, ECONNREFUSED

TCP_ACCEPT_ERRORS = (EMFILE, ENOBUFS, ENFILE, ENOMEM, ECONNABORTED)

SOCKET_INTERRUPT_ERRORS = (EINTR, ECONNRESET)
        
class TCPSocket(Socket):
    TYPE = socket.SOCK_STREAM
    FAMILY = socket.AF_INET
    
    def __repr__(self):
        address = self.address
        if address:
            return '%s:%s' % address
        else:
            return 'tcp:closed'
        
    def accept(self):
        sock, addr = self._sock.accept()
        return wrap_socket(self.TYPE, sock), addr
        
        
class TCP6Socket(TCPSocket):
    FAMILY = socket.AF_INET6
    
    def __repr__(self):
        address = self.address
        if address:
            return '[%s]:%s' % address[:2]
        else:
            return 'tcp6:closed'
        
    def _set_options(self, bindto=False, address=None):
        super(TCP6Socket, self)._set_options(bindto, address)
        if platform.type == "posix" and sys.platform != "cygwin":
            # Required: Forces listenTCP6 to listen exclusively on IPv6 addresses.
            # See: http://www.velocityreviews.com/forums/t328345-ipv6-question.html
            self._sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    
    
class UDPSocket(Socket):
    TYPE = socket.SOCK_DGRAM
    FAMILY = socket.AF_INET
    
    

if not platform.is_windows:
    
    class UnixSocket(TCPSocket):
        FAMILY = socket.AF_UNIX
        
        def __repr__(self):
            address = self.address
            if address:
                return "unix:%s" % self.address
            else:
                return 'unix:closed'
        
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

def nice_address(address, family=None):
    if isinstance(address, tuple):
        return ':'.join((str(s) for s in address[:2]))
    elif family:
        return '%s:%s' % (family, address)
    else:
        return address
    
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
    
def parse_connection_string(netloc, default_port=8000):
    """Converts the ``netloc`` into a three elements tuple
``(scheme, host, params)`` where ``scheme`` is a string, ``host`` could
be a string or a two elements tuple (for a tcp address) and ``params`` a
dictionary of parameters. For example::

    parse_connection_string('http://127.0.0.1:9080')
    
returns::

    ('http', ('127.0.0.1', 9080), {})
    
and this example::

    parse_connection_string('redis://127.0.0.1:6379?db=3&password=bla')
    
returns::

    ('redis', ('127.0.0.1', 6379), {'db': '3', 'password': 'bla'})
"""
    if '://' not in netloc:
        netloc = 'dummy://%s' % netloc
    scheme, host, path, query, fragment = urlsplit(netloc)
    if not scheme and not host:
        host, path = path, ''
    elif path and not query:
        query, path = path, ''
        if query:
            if query.find('?'):
                path = query
            else:
                query = query[1:]
    if path:
        raise ValueError("Address must not have a path. Found '%s'" % path)
    if query:
        params = dict(parse_qsl(query))
    else:
        params = {}
    if scheme == 'dummy':
        scheme = ''
    return scheme, parse_address(host, default_port), params

def get_connection_string(scheme, address, params):
    address = ':'.join((str(b) for b in address))
    if params:
        address += '?' + urlencode(params)
    return scheme + '://' + address
    
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
    
def wrap_socket(type, sock, timeout=0):
    '''Wrap a python socket with pulsar :class:`Socket`.'''
    if sock and not isinstance(sock, Socket):
        sock = get_transport_type(type).family[sock.family](sock)
        sock.settimeout(timeout)
    return sock

def socket_pair(backlog=2048, blocking=0):
    '''Create a ``127.0.0.1`` (client,server) socket pair on any
available port. The first socket is connected to the second, the server socket,
which is bound to ``127.0.0.1`` at any available port.

:param backlog: number of connection to listen.
:rtype: tuple with two instances of :class:`Socket`
'''
    count = 0
    while 1:
        count += 1
        server = create_socket(address=('127.0.0.1', 0), bindto=True,
                               backlog=backlog)
        try:
            # Connect the remote socket with the server socket
            remote_client = create_socket(address=server.address)
            remote_client.connect(server.address)
            break
        except socket.error as e:
            if e[0] != EADDRINUSE:
                raise
            if count >= 10:
                remote_client.close()
                server.close()
                raise socket.error("Cannot bind socket pairs!")
            remote_client.close()
    remote_client.setblocking(blocking)
    return remote_client, server

def is_closed(sock):    #pragma nocover
    """Check if socket is connected."""
    if not sock:
        return False
    try:
        if not poll:
            if not select:
                return False
            try:
                return bool(select([sock], [], [], 0.0)[0])
            except socket.error:
                return True
        # This version is better on platforms that support it.
        p = poll()
        p.register(sock, POLLIN)
        for (fno, ev) in p.poll(0.0):
            if fno == sock.fileno():
                # Either data is buffered (bad), or the connection is dropped.
                return True
    except Exception:
        return True

