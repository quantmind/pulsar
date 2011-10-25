import time
import textwrap
import socket

from pulsar import ispy3k
from pulsar.utils.collections import DictPropertyMixin

CHUNK_SIZE = (16 * 1024)
CHARSET = 'iso-8859-1'
ERRORS='strict'
MAX_BODY = 1024 * 132

weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
monthname = [None,
             'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Server and Date aren't technically hop-by-hop
# headers, but they are in the purview of the
# origin server which the WSGI spec says we should
# act like. So we drop them and add our own.
#
# In the future, concatenation server header values
# might be better, but nothing else does it and
# dropping them is easier.
HOP_HEADERS = set("""
    connection keep-alive proxy-authenticate proxy-authorization
    te trailers transfer-encoding upgrade
    server date
    """.split())


if ispy3k:
    
    def to_string(data, charset=CHARSET, errors=ERRORS):
        if isinstance(data,bytes):
            return data.decode(charset,errors)
        elif not isinstance(data,str):
            return str(data)
        return data
    
    
    def to_bytes(data, charset=CHARSET, errors=ERRORS):
        if isinstance(data,str):
            return data.encode(charset,errors)
        return data

else:
    # Don't bother
    def to_string(data, charset=CHARSET, errors=ERRORS):
        return unicode(data)
    
    def to_bytes(data, charset=CHARSET, errors=ERRORS):
        return str(data)
    
    
def is_hoppish(header):
    return header.lower().strip() in HOP_HEADERS


def http_date(timestamp=None):
    """Return the current date and time formatted for a message header."""
    if timestamp is None:
        timestamp = time.time()
    year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
    s = "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
            weekdayname[wd],
            day, monthname[month], year,
            hh, mm, ss)
    return s


def close(sock):
    if sock:
        try:
            sock.close()
        except socket.error:
            pass
    

def write_chunk(sock, data):
    chunk = "".join(("%X\r\n" % len(data), data, "\r\n"))
    sock.sendall(to_bytes(chunk))

    
def write(sock, data, chunked=False):
    if chunked:
        write_chunk(sock, data)
    else:
        sock.sendall(to_bytes(data))


def write_nonblock(sock, data, chunked=False):
    timeout = sock.gettimeout()
    if timeout != 0.0:
        try:
            sock.setblocking(0)
            return write(sock, data, chunked)
        finally:
            sock.setblocking(1)
    else:
        return write(sock, data, chunked)
    
    
def writelines(sock, lines, chunked=False):
    for line in list(lines):
        write(sock, line, chunked)


def write_error(sock, msg):
    html = textwrap.dedent("""\
    <html>
        <head>
            <title>Internal Server Error</title>
        </head>
        <body>
            <h1>Internal Server Error</h1>
            <h2>WSGI Error Report:</h2>
            <pre>%s</pre>
        </body>
    </html>
    """) % msg
    http = textwrap.dedent("""\
    HTTP/1.1 500 Internal Server Error\r
    Connection: close\r
    Content-Type: text/html\r
    Content-Length: %d\r
    \r
    %s
    """) % (len(html), html)
    write_nonblock(sock, http)
    
    
class Authorization(DictPropertyMixin):
    """Represents an `Authorization` header sent by the client.  You should
    not create this kind of object yourself but use it when it's returned by
    the `parse_authorization_header` function.

    This object is a dict subclass and can be altered by setting dict items
    but it should be considered immutable as it's returned by the client and
    not meant for modifications.

    .. versionchanged:: 0.5
       This object became immutable.
    """

    def __init__(self, auth_type, data=None):
        super(Authorization,self).__init__(data)
        self.type = auth_type



def parse_list_header(value):
    """Parse lists as described by RFC 2068 Section 2.

    In particular, parse comma-separated lists where the elements of
    the list may include quoted-strings.  A quoted-string could
    contain a comma.  A non-quoted string could have quotes in the
    middle.  Quotes are removed automatically after parsing.

    It basically works like :func:`parse_set_header` just that items
    may appear multiple times and case sensitivity is preserved.

    The return value is a standard :class:`list`:

    >>> parse_list_header('token, "quoted value"')
    ['token', 'quoted value']

    To create a header from the :class:`list` again, use the
    :func:`dump_header` function.

    :param value: a string with a list header.
    :return: :class:`list`
    """
    result = []
    for item in _parse_list_header(value):
        if item[:1] == item[-1:] == '"':
            item = unquote_header_value(item[1:-1])
        result.append(item)
    return result


def parse_dict_header(value):
    """Parse lists of key, value pairs as described by RFC 2068 Section 2 and
    convert them into a python dict:

    >>> d = parse_dict_header('foo="is a fish", bar="as well"')
    >>> type(d) is dict
    True
    >>> sorted(d.items())
    [('bar', 'as well'), ('foo', 'is a fish')]

    If there is no value for a key it will be `None`:

    >>> parse_dict_header('key_without_value')
    {'key_without_value': None}

    To create a header from the :class:`dict` again, use the
    :func:`dump_header` function.

    :param value: a string with a dict header.
    :return: :class:`dict`
    """
    result = {}
    for item in _parse_list_header(value):
        if '=' not in item:
            result[item] = None
            continue
        name, value = item.split('=', 1)
        if value[:1] == value[-1:] == '"':
            value = unquote_header_value(value[1:-1])
        result[name] = value
    return result


def __parse_authorization_header(value):
    """Parse an HTTP basic/digest authorization header transmitted by the web
    browser.  The return value is either `None` if the header was invalid or
    not given, otherwise an :class:`Authorization` object.

    :param value: the authorization header to parse.
    :return: a :class:`Authorization` object or `None`.
    """
    if not value:
        return
    try:
        auth_type, auth_info = value.split(None, 1)
        auth_type = auth_type.lower()
    except ValueError:
        return
    if auth_type == 'basic':
        try:
            username, password = auth_info.decode('base64').split(':', 1)
        except Exception as e:
            return
        return Authorization('basic', {'username': username,
                                       'password': password})
    elif auth_type == 'digest':
        auth_map = parse_dict_header(auth_info)
        for key in 'username', 'realm', 'nonce', 'uri', 'nc', 'cnonce', \
                   'response':
            if not key in auth_map:
                return
        return Authorization('digest', auth_map)
    
