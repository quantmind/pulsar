import socket
from urllib.parse import urlsplit, parse_qsl, urlencode

try:
    from select import poll, POLLIN
except ImportError:  # pragma    nocover
    poll = None
    try:
        from select import select
    except ImportError:  # pragma    nocover
        select = False


from .string import native_str


def parse_address(netloc, default_port=8000):
    '''Parse an internet address ``netloc`` and return a tuple with
``host`` and ``port``.'''
    if isinstance(netloc, tuple):
        if len(netloc) != 2:
            raise ValueError('Invalid address %s' % str(netloc))
        return netloc
    #
    netloc = native_str(netloc)
    auth = None
    # Check if auth is available
    if '@' in netloc:
        auth, netloc = netloc.split('@')
    if netloc.startswith("unix:"):
        host = netloc.split("unix:")[1]
        return '%s@%s' % (auth, host) if auth else host
    # get host
    if '[' in netloc and ']' in netloc:
        host = netloc.split(']')[0][1:].lower()
    elif ':' in netloc:
        host = netloc.split(':')[0].lower()
    elif netloc == "":
        host = "0.0.0.0"
    else:
        host = netloc.lower()
    # get port
    netloc = netloc.split(']')[-1]
    if ":" in netloc:
        port = netloc.split(':', 1)[1]
        if not port.isdigit():
            raise ValueError("%r is not a valid port number." % port)
        port = int(port)
    else:
        port = default_port
    return ('%s@%s' % (auth, host) if auth else host, port)


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


def get_connection_string(scheme, address, params):
    address = ':'.join((str(b) for b in address))
    if params:
        address += '?' + urlencode(params)
    return scheme + '://' + address


def is_socket_closed(sock):
    """Check if socket ``sock`` is closed."""
    if not sock:
        return True
    try:
        if not poll:    # pragma nocover
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


def close_socket(sock):
    '''Shutdown and close the socket.'''
    if sock:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass


def nice_address(address, family=None):
    if isinstance(address, tuple):
        address = ':'.join((str(s) for s in address[:2]))
    return '%s %s' % (family, address) if family else address


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


def is_tls(sock):
    '''Check if ``sock`` is a socket over transport layer security
    '''
    try:
        import ssl
        return isinstance(sock, ssl.SSLSocket)
    except ImportError:
        return False
