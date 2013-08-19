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
from .httpurl import urlsplit, parse_qsl, urlencode
from .pep import native_str

WRITE_BUFFER_MAX_SIZE = 128 * 1024  # 128 kb

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

ACCEPT_ERRORS = (EMFILE, ENOBUFS, ENFILE, ENOMEM, ECONNABORTED)
TRY_WRITE_AGAIN = (EWOULDBLOCK, ENOBUFS, EINPROGRESS)
TRY_READ_AGAIN = (EWOULDBLOCK, EAGAIN)

SOCKET_INTERRUPT_ERRORS = (EINTR, ECONNRESET)


def parse_address(netloc, default_port=8000):
    '''Parse an internet address ``netloc`` and return a tuple with
``host`` and ``port``.'''
    if isinstance(netloc, tuple):
        if len(netloc) != 2:
            raise ValueError('Invalid address %s' % str(netloc))
        return netloc
    #
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

def parse_connection_string(connection_string, default_port=8000):
    """Converts the ``connection_string`` into a three elements tuple
``(scheme, host, params)`` where ``scheme`` is a string, ``host`` could
be a string or a two elements tuple (for a tcp address) and ``params`` a
dictionary of parameters. The ``default_port`` parameter can be used to
set the port if a port is not available in the ``connection_string``.

For example::

    >>> parse_connection_string('http://127.0.0.1:9080')
    ('http', ('127.0.0.1', 9080), {})
    
and this example::

    >>> parse_connection_string('redis://127.0.0.1:6379?db=3&password=bla')
    ('redis', ('127.0.0.1', 6379), {'db': '3', 'password': 'bla'})
"""
    if '://' not in connection_string:
        connection_string = 'dummy://%s' % connection_string
    scheme, host, path, query, fragment = urlsplit(connection_string)
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

def is_closed(sock):
    """Check if socket ``sock`` is closed."""
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

def nice_address(address, family=None):
    if isinstance(address, tuple):
        return ':'.join((str(s) for s in address[:2]))
    elif family:
        return '%s:%s' % (family, address)
    else:
        return address
    
def format_address(address):
    if isinstance(address, tuple):
        if len(address) == 2:
            return '%s:%s' % address
        elif len(address) == 4:
            return '[%s]:%s' % address[:2]
        else:
            raise ValueError('Could not format address %s' % str(address))
    else:
        return str(address)