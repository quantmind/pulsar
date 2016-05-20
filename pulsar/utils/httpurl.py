'''This is a substantial module which imporfts several classes and functions
from the standard library in a python 2.6 to python 3.3 compatible fashion.
On top of that, it implements the :class:`HttpClient` for handling synchronous
and asynchronous HTTP requests in a pythonic way.

It is a thin layer on top of urllib2 in python2 / urllib in Python 3.
Several opensource efforts have been used as source of snippets:

* http-parser_
* request_
* urllib3_
* werkzeug_


.. _tools-http-headers:

HTTP Headers
~~~~~~~~~~~~~~~~~

.. autoclass:: Headers
   :members:
   :member-order: bysource


.. _tools-http-parser:

HTTP Parser
~~~~~~~~~~~~~~~~~

.. autoclass:: HttpParser
   :members:
   :member-order: bysource


.. _http-parser: https://github.com/benoitc/http-parser
.. _urllib3: https://github.com/shazow/urllib3
.. _request: https://github.com/kennethreitz/requests
.. _werkzeug: https://github.com/mitsuhiko/werkzeug
.. _`HTTP cookie`: http://en.wikipedia.org/wiki/HTTP_cookie
'''
import os
import sys
import re
import string
import mimetypes
from hashlib import sha1, md5
from uuid import uuid4
from email.utils import formatdate
from io import BytesIO
import zlib
from collections import deque, OrderedDict
from urllib import request as urllibr
from http import client as httpclient
from urllib.parse import quote, urlsplit, splitport
from http.cookiejar import CookieJar, Cookie
from http.cookies import SimpleCookie

from .structures import mapping_iterator
from .string import to_bytes, to_string
from .html import capfirst
#
# The http_parser has several bugs, therefore it is switched off
hasextensions = False
_Http_Parser = None


def setDefaultHttpParser(parser):   # pragma    nocover
    global _Http_Parser
    _Http_Parser = parser


def http_parser(**kwargs):
    global _Http_Parser
    return _Http_Parser(**kwargs)


getproxies_environment = urllibr.getproxies_environment
ascii_letters = string.ascii_letters
HTTPError = urllibr.HTTPError
URLError = urllibr.URLError
parse_http_list = urllibr.parse_http_list


# ###################################################    URI & IRI SUFF
#
# The reserved URI characters (RFC 3986 - section 2.2)
# Default is charset is "iso-8859-1" (latin-1) from section 3.7.1
# http://www.ietf.org/rfc/rfc2616.txt
DEFAULT_CHARSET = 'ISO-8859-1'
URI_GEN_DELIMS = frozenset(':/?#[]@')
URI_SUB_DELIMS = frozenset("!$&'()*+,;=")
URI_RESERVED_SET = URI_GEN_DELIMS.union(URI_SUB_DELIMS)
URI_RESERVED_CHARS = ''.join(URI_RESERVED_SET)
# The unreserved URI characters (RFC 3986 - section 2.3)
URI_UNRESERVED_SET = frozenset(ascii_letters + string.digits + '-._~')
URI_SAFE_CHARS = URI_RESERVED_CHARS + '%~'
HEADER_TOKEN_CHARS = frozenset("!#$%&'*+-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               '^_`abcdefghijklmnopqrstuvwxyz|~')
MAX_CHUNK_SIZE = 65536


def escape(s):
    return quote(s, safe='~')


def urlquote(iri):
    return quote(iri, safe=URI_RESERVED_CHARS)


def _gen_unquote(uri):
    unreserved_set = URI_UNRESERVED_SET
    for n, part in enumerate(to_string(uri, 'latin1').split('%')):
        if not n:
            yield part
        else:
            h = part[0:2]
            if len(h) == 2:
                c = chr(int(h, 16))
                if c in unreserved_set:
                    yield c + part[2:]
                else:
                    yield '%' + part
            else:
                yield '%' + part


def unquote_unreserved(uri):
    """Un-escape any percent-escape sequences in a URI that are unreserved
characters. This leaves all reserved, illegal and non-ASCII bytes encoded."""
    return ''.join(_gen_unquote(uri))


def requote_uri(uri):
    """Re-quote the given URI.

    This function passes the given URI through an unquote/quote cycle to
    ensure that it is fully and consistently quoted.
    """
    # Unquote only the unreserved characters
    # Then quote only illegal characters (do not quote reserved, unreserved,
    # or '%')
    return quote(unquote_unreserved(uri), safe=URI_SAFE_CHARS)


def iri_to_uri(iri, kwargs=None):
    '''Convert an Internationalised Resource Identifier (IRI) portion
    to a URI portion that is suitable for inclusion in a URL.
    This is the algorithm from section 3.1 of RFC 3987.
    Returns an ASCII native string containing the encoded result.
    '''
    if iri is None:
        return iri
    if kwargs:
        iri = '%s?%s' % (to_string(iri, 'latin1'),
                         '&'.join(('%s=%s' % kv for kv in kwargs.items())))
    return urlquote(unquote_unreserved(iri))


def host_and_port(host):
    host, port = splitport(host)
    return host, int(port) if port else None


def default_port(scheme):
    if scheme in ("http", "ws"):
        return '80'
    elif scheme in ("https", "wss"):
        return '443'


def host_and_port_default(scheme, host):
    host, port = splitport(host)
    if not port:
        port = default_port(scheme)
    return host, port


def host_no_default_port(scheme, netloc):
    host, port = splitport(netloc)
    if port and port == default_port(scheme):
        return host
    else:
        return netloc


def get_hostport(scheme, full_host):
    host, port = host_and_port(full_host)
    if port is None:
        i = host.rfind(':')
        j = host.rfind(']')         # ipv6 addresses have [...]
        if i > j:
            try:
                port = int(host[i+1:])
            except ValueError:
                if host[i+1:] == "":  # http://foo.com:/ == http://foo.com/
                    port = default_port(scheme)
                else:
                    raise httpclient.InvalidURL("nonnumeric port: '%s'"
                                                % host[i+1:])
            host = host[:i]
        else:
            port = default_port(scheme)
        if host and host[0] == '[' and host[-1] == ']':
            host = host[1:-1]
    return host, int(port)


def remove_double_slash(route):
    if '//' in route:
        route = re.sub('/+', '/', route)
    return route


# ###################################################    CONTENT TYPES
JSON_CONTENT_TYPES = ('application/json',
                      'application/javascript',
                      'text/json',
                      'text/x-json')
# ###################################################    REQUEST METHODS
ENCODE_URL_METHODS = frozenset(['DELETE', 'GET', 'HEAD', 'OPTIONS'])
ENCODE_BODY_METHODS = frozenset(['PATCH', 'POST', 'PUT', 'TRACE'])
REDIRECT_CODES = (301, 302, 303, 305, 307)


def has_empty_content(status, method=None):
    '''204, 304 and 1xx codes have no content'''
    if status == httpclient.NO_CONTENT or\
            status == httpclient.NOT_MODIFIED or\
            100 <= status < 200 or\
            method == "HEAD":
        return True
    else:
        return False


def is_succesful(status):
    '''2xx status is succesful'''
    return status >= 200 and status < 300


# ###################################################    HTTP HEADERS
WEBSOCKET_VERSION = (8, 13)
HEADER_FIELDS = {'general': frozenset(('Cache-Control', 'Connection', 'Date',
                                       'Pragma', 'Trailer',
                                       'Transfer-Encoding',
                                       'Upgrade', 'Sec-WebSocket-Extensions',
                                       'Sec-WebSocket-Protocol',
                                       'Via', 'Warning')),
                 # The request-header fields allow the client to pass
                 # additional information about the request, and about the
                 # client to the server.
                 'request': frozenset(('Accept', 'Accept-Charset',
                                       'Accept-Encoding', 'Accept-Language',
                                       'Authorization',
                                       'Cookie', 'Expect', 'From',
                                       'Host', 'If-Match', 'If-Modified-Since',
                                       'If-None-Match', 'If-Range',
                                       'If-Unmodified-Since', 'Max-Forwards',
                                       'Proxy-Authorization', 'Range',
                                       'Referer',
                                       'Sec-WebSocket-Key',
                                       'Sec-WebSocket-Version',
                                       'TE',
                                       'User-Agent',
                                       'X-Requested-With')),
                 # The response-header fields allow the server to pass
                 # additional information about the response which cannot be
                 # placed in the Status- Line.
                 'response': frozenset(('Accept-Ranges',
                                        'Age',
                                        'ETag',
                                        'Location',
                                        'Proxy-Authenticate',
                                        'Retry-After',
                                        'Sec-WebSocket-Accept',
                                        'Server',
                                        'Set-Cookie',
                                        'Set-Cookie2',
                                        'Vary',
                                        'WWW-Authenticate',
                                        'X-Frame-Options')),
                 'entity': frozenset(('Allow', 'Content-Encoding',
                                      'Content-Language', 'Content-Length',
                                      'Content-Location', 'Content-MD5',
                                      'Content-Range', 'Content-Type',
                                      'Expires', 'Last-Modified'))}

CLIENT_HEADER_FIELDS = HEADER_FIELDS['general'].union(
    HEADER_FIELDS['entity'], HEADER_FIELDS['request'])
SERVER_HEADER_FIELDS = HEADER_FIELDS['general'].union(
    HEADER_FIELDS['entity'], HEADER_FIELDS['response'])
ALL_HEADER_FIELDS = CLIENT_HEADER_FIELDS.union(SERVER_HEADER_FIELDS)
ALL_HEADER_FIELDS_DICT = dict(((k.lower(), k) for k in ALL_HEADER_FIELDS))
CRLF = '\r\n'
LWS = '\r\n '
TYPE_HEADER_FIELDS = {'client': CLIENT_HEADER_FIELDS,
                      'server': SERVER_HEADER_FIELDS,
                      'both': ALL_HEADER_FIELDS}

header_type = {0: 'client', 1: 'server', 2: 'both'}
header_type_to_int = dict(((v, k) for k, v in header_type.items()))


def capheader(name):
    return '-'.join((b for b in (capfirst(n) for n in name.split('-')) if b))


def header_field(name, headers=None, strict=False):
    '''Return a header `name` in Camel case.

    For example::

        header_field('connection') == 'Connection'
        header_field('accept-charset') == 'Accept-Charset'

    If ``header_set`` is given, only return headers included in the set.
    '''
    name = name.lower()
    if name.startswith('x-') or not strict:
        return capheader(name)
    else:
        header = ALL_HEADER_FIELDS_DICT.get(name)
        if header and headers:
            return header if header in headers else None
        else:
            return header


#    HEADERS UTILITIES
HEADER_FIELDS_JOINER = {'Cookie': '; ',
                        'Set-Cookie': None,
                        'Set-Cookie2': None}


def split_comma(value):
    return [v for v in (v.strip() for v in value.split(',')) if v]


def parse_cookies(value):
    return [c.OutputString() for c in SimpleCookie(value).values()]


header_parsers = {'Connection': split_comma,
                  'Cookie': parse_cookies}


def header_values(header, value):
    assert isinstance(value, str)
    if header in header_parsers:
        return header_parsers[header](value)
    else:
        return [value]


def quote_header_value(value, extra_chars='', allow_token=True):
    """Quote a header value if necessary.

    :param value: the value to quote.
    :param extra_chars: a list of extra characters to skip quoting.
    :param allow_token: if this is enabled token values are returned
        unchanged.
    """
    value = to_string(value)
    if allow_token:
        token_chars = HEADER_TOKEN_CHARS | set(extra_chars)
        if set(value).issubset(token_chars):
            return value
    return '"%s"' % value.replace('\\', '\\\\').replace('"', '\\"')


def unquote_header_value(value, is_filename=False):
    """Unquotes a header value.

    Reversal of :func:`quote_header_value`. This does not use the real
    un-quoting but what browsers are actually using for quoting.

    :param value: the header value to unquote.
    """
    if value and value[0] == value[-1] == '"':
        # this is not the real unquoting, but fixing this so that the
        # RFC is met will result in bugs with internet explorer and
        # probably some other browsers as well.  IE for example is
        # uploading files with "C:\foo\bar.txt" as filename
        value = value[1:-1]
        # if this is a filename and the starting characters look like
        # a UNC path, then just return the value without quotes.  Using the
        # replace sequence below on a UNC path has the effect of turning
        # the leading double slash into a single slash and then
        # _fix_ie_filename() doesn't work correctly.  See #458.
        if not is_filename or value[:2] != '\\\\':
            return value.replace('\\\\', '\\').replace('\\"', '"')
    return value


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
    for item in parse_http_list(value):
        if '=' not in item:
            result[item] = None
            continue
        name, value = item.split('=', 1)
        if value[:1] == value[-1:] == '"':
            value = unquote_header_value(value[1:-1])
        result[name] = value
    return result


_special = re.escape('()<>@,;:\\"/[]?={} \t')
_re_special = re.compile('[%s]' % _special)
_qstr = '"(?:\\\\.|[^"])*"'  # Quoted string
_value = '(?:[^%s]+|%s)' % (_special, _qstr)  # Save or quoted string
_option = '(?:;|^)\s*([^%s]+)\s*=\s*(%s)' % (_special, _value)
_re_option = re.compile(_option)  # key=value part of an Content-Type header


def header_unquote(val, filename=False):
    if val[0] == val[-1] == '"':
        val = val[1:-1]
        if val[1:3] == ':\\' or val[:2] == '\\\\':
            val = val.split('\\')[-1]  # fix ie6 bug: full path --> filename
        return val.replace('\\\\', '\\').replace('\\"', '"')
    return val


def parse_options_header(header, options=None):
    if ';' not in header:
        return header.lower().strip(), {}
    ctype, tail = header.split(';', 1)
    options = options or {}
    for match in _re_option.finditer(tail):
        key = match.group(1).lower()
        value = header_unquote(match.group(2), key == 'filename')
        options[key] = value
    return ctype, options


class Headers:
    '''Utility for managing HTTP headers for both clients and servers.

    It has a dictionary like interface with few extra functions to facilitate
    the insertion of multiple header values. Header fields are
    **case insensitive**, therefore doing::

        >>> h = Headers()
        >>> h['Content-Length'] = '1050'

    is equivalent to

        >>> h['content-length'] = '1050'

    :param headers: optional iterable over header field/value pairs.
    :param kind: optional headers type, one of ``server``, ``client`` or
        ``both``.
    :param strict: if ``True`` only valid headers field will be included.

    This :class:`Headers` container maintains an ordering as suggested by
    http://www.w3.org/Protocols/rfc2616/rfc2616.html:

    .. epigraph::

        The order in which header fields with differing field names are
        received is not significant. However, it is "good practice" to send
        general-header fields first, followed by request-header or
        response-header fields, and ending with the entity-header fields.

        -- rfc2616 section 4.2

    The strict parameter is rarely used and it forces the omission on
    non-standard header fields.
    '''
    @classmethod
    def make(cls, headers):
        if not isinstance(headers, cls):
            headers = cls(headers=headers)
        return headers

    def __init__(self, headers=None, kind='server', strict=False):
        if isinstance(kind, int):
            kind = header_type.get(kind, 'both')
        else:
            kind = str(kind).lower()
        self.kind = kind
        self.strict = strict
        self.all_headers = TYPE_HEADER_FIELDS.get(self.kind)
        if not self.all_headers:
            self.kind = 'both'
            self.all_headers = TYPE_HEADER_FIELDS[self.kind]
        self._headers = {}
        if headers is not None:
            self.update(headers)

    def __repr__(self):
        return '%s %s' % (self.kind, self._headers.__repr__())

    def __str__(self):
        return '\r\n'.join(self._ordered())

    def __bytes__(self):
        return str(self).encode(DEFAULT_CHARSET)

    def __len__(self):
        return len(self._headers)

    @property
    def kind_number(self):
        return header_type_to_int.get(self.kind)

    def update(self, iterable):
        """Extend the headers with an ``iterable``.

        :param iterable: a dictionary or an iterable over keys, vaues tuples.
        """
        for key, value in mapping_iterator(iterable):
            self.add_header(key, value)

    def override(self, iterable):
        '''Extend headers by overriding fields form iterable.

        :param iterable: a dictionary or an iterable over keys, vaues tuples.
        '''
        seen = set()
        for key, value in mapping_iterator(iterable):
            key = key.lower()
            if key in seen:
                self.add_header(key, value)
            else:
                seen.add(key)
                self[key] = value

    def copy(self):
        return self.__class__(self, kind=self.kind, strict=self.strict)

    def __contains__(self, key):
        return header_field(key) in self._headers

    def __getitem__(self, key):
        key = header_field(key)
        values = self._headers[key]
        joiner = HEADER_FIELDS_JOINER.get(key, ', ')
        if joiner is None:
            joiner = '; '
        return joiner.join(values)

    def __delitem__(self, key):
        self._headers.__delitem__(header_field(key))

    def __setitem__(self, key, value):
        key = header_field(key, self.all_headers, self.strict)
        if key and value:
            if not isinstance(value, list):
                value = header_values(key, value)
            self._headers[key] = value

    def get(self, key, default=None):
        '''Get the field value at ``key`` as comma separated values.

        For example::

            >>> from pulsar.utils.httpurl import Headers
            >>> h = Headers(kind='client')
            >>> h.add_header('accept-encoding', 'gzip')
            >>> h.add_header('accept-encoding', 'deflate')
            >>> h.get('accept-encoding')

        results in::

            'gzip, deflate'
        '''
        if key in self:
            return self.__getitem__(key)
        else:
            return default

    def get_all(self, key, default=None):
        '''Get the values at header ``key`` as a list rather than a
        string separated by comma (which is returned by the
        :meth:`get` method).

        For example::

            >>> from pulsar.utils.httpurl import Headers
            >>> h = Headers(kind='client')
            >>> h.add_header('accept-encoding', 'gzip')
            >>> h.add_header('accept-encoding', 'deflate')
            >>> h.get_all('accept-encoding')

        results in::

            ['gzip', 'deflate']
        '''
        return self._headers.get(header_field(key), default)

    def has(self, field, value):
        '''Check if ``value`` is avialble in header ``field``.'''
        value = value.lower()
        for c in self.get_all(field, ()):
            if c.lower() == value:
                return True
        return False

    def pop(self, key, *args):
        return self._headers.pop(header_field(key), *args)

    def clear(self):
        '''Same as :meth:`dict.clear`, it removes all headers.
        '''
        self._headers.clear()

    def getheaders(self, key):  # pragma    nocover
        '''Required by cookielib in python 2.

        If the key is not available, it returns an empty list.
        '''
        return self._headers.get(header_field(key), [])

    def add_header(self, key, values):
        '''Add ``values`` to ``key`` header.

        If the header is already available, append the value to the list.

        :param key: header name
        :param values: a string value or a list/tuple of strings values
            for header ``key``
        '''
        key = header_field(key, self.all_headers, self.strict)
        if key and values:
            if not isinstance(values, (tuple, list)):
                values = header_values(key, values)
            current = self._headers.get(key, [])
            for value in values:
                if value and value not in current:
                    current.append(value)
            self._headers[key] = current

    def remove_header(self, key, value=None):
        '''Remove the header at ``key``.

        If ``value`` is provided, it removes only that value if found.
        '''
        key = header_field(key, self.all_headers, self.strict)
        if key:
            if value:
                value = value.lower()
                values = self._headers.get(key, [])
                removed = None
                for v in values:
                    if v.lower() == value:
                        removed = v
                        values.remove(v)
                self._headers[key] = values
                return removed
            else:
                return self._headers.pop(key, None)

    def flat(self, version, status):
        '''Full headers bytes representation'''
        vs = version + (status, self)
        return ('HTTP/%s.%s %s\r\n%s' % vs).encode(DEFAULT_CHARSET)

    def __iter__(self):
        dj = ', '
        for k, values in self._headers.items():
            joiner = HEADER_FIELDS_JOINER.get(k, dj)
            if joiner:
                yield k, joiner.join(values)
            else:
                for value in values:
                    yield k, value

    def _ordered(self):
        hf = HEADER_FIELDS
        hj = HEADER_FIELDS_JOINER
        dj = ', '
        order = (('general', []), ('request', []),
                 ('response', []), ('entity', []))
        headers = self._headers
        for key in headers:
            for name, group in order:
                if key in hf[name]:
                    group.append(key)
                    break
            if key not in group:    # non-standard header
                group.append(key)
        for _, group in order:
            for k in group:
                joiner = hj.get(k, dj)
                if not joiner:
                    for header in headers[k]:
                        yield "%s: %s" % (k, header)
                else:
                    yield "%s: %s" % (k, joiner.join(headers[k]))
        yield ''
        yield ''


###############################################################################
#    HTTP PARSER
###############################################################################
METHOD_RE = re.compile("[A-Z0-9$-_.]{3,20}")
VERSION_RE = re.compile("HTTP/(\d+).(\d+)")
STATUS_RE = re.compile("(\d{3})\s*(\w*)")
HEADER_RE = re.compile("[\x00-\x1F\x7F()<>@,;:\[\]={} \t\\\\\"]")

# errors
BAD_FIRST_LINE = 0
INVALID_HEADER = 1
INVALID_CHUNK = 2


class InvalidRequestLine(Exception):
    """ error raised when first line is invalid """


class InvalidHeader(Exception):
    """ error raised on invalid header """


class InvalidChunkSize(Exception):
    """ error raised when we parse an invalid chunk size """


class HttpParser:
    '''A python HTTP parser.

    Original code from https://github.com/benoitc/http-parser

    2011 (c) Benoit Chesneau <benoitc@e-engura.org>
    '''
    def __init__(self, kind=2, decompress=False, method=None):
        self.decompress = decompress
        # errors vars
        self.errno = None
        self.errstr = ""
        # protected variables
        self._buf = []
        self._version = None
        self._method = method
        self._status_code = None
        self._status = None
        self._reason = None
        self._url = None
        self._path = None
        self._query_string = None
        self._kind = kind
        self._fragment = None
        self._headers = OrderedDict()
        self._chunked = False
        self._body = []
        self._trailers = None
        self._partial_body = False
        self._clen = None
        self._clen_rest = None
        # private events
        self.__on_firstline = False
        self.__on_headers_complete = False
        self.__on_message_begin = False
        self.__on_message_complete = False
        # decompress
        self.__decompress_obj = None
        self.__decompress_first_try = True

    @property
    def kind(self):
        return self._kind

    def get_version(self):
        return self._version

    def get_method(self):
        return self._method

    def get_status_code(self):
        return self._status_code

    def get_url(self):
        return self._url

    def get_path(self):
        return self._path

    def get_query_string(self):
        return self._query_string

    def get_fragment(self):
        return self._fragment

    def get_headers(self):
        return self._headers

    def recv_body(self):
        """ return last chunk of the parsed body"""
        body = b''.join(self._body)
        self._body = []
        self._partial_body = False
        return body

    def is_headers_complete(self):
        """ return True if all headers have been parsed. """
        return self.__on_headers_complete

    def is_partial_body(self):
        """ return True if a chunk of body have been parsed """
        return self._partial_body

    def is_message_begin(self):
        """ return True if the parsing start """
        return self.__on_message_begin

    def is_message_complete(self):
        """ return True if the parsing is done (we get EOF) """
        return self.__on_message_complete

    def is_chunked(self):
        """ return True if Transfer-Encoding header value is chunked"""
        return self._chunked

    def execute(self, data, length):
        # end of body can be passed manually by putting a length of 0
        if length == 0:
            self.__on_message_complete = True
            return length
        #
        data = bytes(data)
        # start to parse
        nb_parsed = 0
        while True:
            if not self.__on_firstline:
                idx = data.find(b'\r\n')
                if idx < 0:
                    self._buf.append(data)
                    return len(data)
                else:
                    self.__on_firstline = True
                    self._buf.append(data[:idx])
                    first_line = to_string(b''.join(self._buf),
                                           DEFAULT_CHARSET)
                    rest = data[idx+2:]
                    data = b''
                    if self._parse_firstline(first_line):
                        nb_parsed = nb_parsed + idx + 2
                        self._buf = [rest]
                    else:
                        return nb_parsed
            elif not self.__on_headers_complete:
                if data:
                    self._buf.append(data)
                    data = b''
                try:
                    to_parse = b''.join(self._buf)
                    ret = self._parse_headers(to_parse)
                    if ret is False:
                        return length
                    nb_parsed = nb_parsed + (len(to_parse) - ret)
                except InvalidHeader as e:
                    self.errno = INVALID_HEADER
                    self.errstr = str(e)
                    return nb_parsed
            elif not self.__on_message_complete:
                self.__on_message_begin = True
                if data:
                    self._buf.append(data)
                    data = b''
                ret = self._parse_body()
                if ret is None:
                    return length
                elif ret < 0:
                    return ret
                elif ret == 0:
                    self.__on_message_complete = True
                    return length
                else:
                    nb_parsed = max(length, ret)
            else:
                return 0

    def _parse_firstline(self, line):
        try:
            if self.kind == 2:  # auto detect
                try:
                    self._parse_request_line(line)
                except InvalidRequestLine:
                    self._parse_response_line(line)
            elif self.kind == 1:
                self._parse_response_line(line)
            elif self.kind == 0:
                self._parse_request_line(line)
        except InvalidRequestLine as e:
            self.errno = BAD_FIRST_LINE
            self.errstr = str(e)
            return False
        return True

    def _parse_response_line(self, line):
        bits = line.split(None, 1)
        if len(bits) != 2:
            raise InvalidRequestLine(line)

        # version
        matchv = VERSION_RE.match(bits[0])
        if matchv is None:
            raise InvalidRequestLine("Invalid HTTP version: %s" % bits[0])
        self._version = (int(matchv.group(1)), int(matchv.group(2)))

        # status
        matchs = STATUS_RE.match(bits[1])
        if matchs is None:
            raise InvalidRequestLine("Invalid status %" % bits[1])

        self._status = bits[1]
        self._status_code = int(matchs.group(1))
        self._reason = matchs.group(2)

    def _parse_request_line(self, line):
        bits = line.split(None, 2)
        if len(bits) != 3:
            raise InvalidRequestLine(line)
        # Method
        if not METHOD_RE.match(bits[0]):
            raise InvalidRequestLine("invalid Method: %s" % bits[0])
        self._method = bits[0].upper()
        # URI
        self._url = bits[1]
        parts = urlsplit('http://dummy.com%s' % bits[1])
        self._path = parts.path or ""
        self._query_string = parts.query or ""
        self._fragment = parts.fragment or ""
        # Version
        match = VERSION_RE.match(bits[2])
        if match is None:
            raise InvalidRequestLine("Invalid HTTP version: %s" % bits[2])
        self._version = (int(match.group(1)), int(match.group(2)))

    def _parse_headers(self, data):
        if data == b'\r\n':
            self.__on_headers_complete = True
            self._buf = []
            return 0
        idx = data.find(b'\r\n\r\n')
        if idx < 0:  # we don't have all headers
            return False
        chunk = to_string(data[:idx], DEFAULT_CHARSET)
        # Split lines on \r\n keeping the \r\n on each line
        lines = deque(('%s\r\n' % line for line in chunk.split('\r\n')))
        # Parse headers into key/value pairs paying attention
        # to continuation lines.
        while len(lines):
            # Parse initial header name : value pair.
            curr = lines.popleft()
            if curr.find(":") < 0:
                continue
            name, value = curr.split(":", 1)
            name = name.rstrip(" \t").upper()
            if HEADER_RE.search(name):
                raise InvalidHeader("invalid header name %s" % name)
            name, value = header_field(name.strip()), [value.lstrip()]
            # Consume value continuation lines
            while len(lines) and lines[0].startswith((" ", "\t")):
                value.append(lines.popleft())
            value = ''.join(value).rstrip()
            if name in self._headers:
                self._headers[name].append(value)
            else:
                self._headers[name] = [value]
        # detect now if body is sent by chunks.
        clen = self._headers.get('Content-Length')
        if 'Transfer-Encoding' in self._headers:
            te = self._headers['Transfer-Encoding'][0].lower()
            self._chunked = (te == 'chunked')
        else:
            self._chunked = False
        #
        status = self._status_code
        if status and (status == httpclient.NO_CONTENT or
                       status == httpclient.NOT_MODIFIED or
                       100 <= status < 200 or      # 1xx codes
                       self._method == "HEAD"):
            clen = 0
        elif clen is not None:
            try:
                clen = int(clen[0])
            except ValueError:
                clen = None
            else:
                if clen < 0:  # ignore nonsensical negative lengths
                    clen = None
        #
        if clen is None:
            self._clen_rest = sys.maxsize
        else:
            self._clen_rest = self._clen = clen
        #
        # detect encoding and set decompress object
        if self.decompress and 'Content-Encoding' in self._headers:
            encoding = self._headers['Content-Encoding'][0]
            if encoding == "gzip":
                self.__decompress_obj = zlib.decompressobj(16+zlib.MAX_WBITS)
                self.__decompress_first_try = False
            elif encoding == "deflate":
                self.__decompress_obj = zlib.decompressobj()

        rest = data[idx+4:]
        self._buf = [rest]
        self.__on_headers_complete = True
        self.__on_message_begin = True
        return len(rest)

    def _parse_body(self):
        data = b''.join(self._buf)
        #
        if not self._chunked:
            #
            if not data and self._clen is None:
                if not self._status:    # message complete only for servers
                    self.__on_message_complete = True
            else:
                if self._clen_rest is not None:
                    self._clen_rest -= len(data)

                # maybe decompress
                data = self._decompress(data)

                self._partial_body = True
                if data:
                    self._body.append(data)
                self._buf = []
                if self._clen_rest <= 0:
                    self.__on_message_complete = True
            return
        else:
            try:
                size, rest = self._parse_chunk_size(data)
            except InvalidChunkSize as e:
                self.errno = INVALID_CHUNK
                self.errstr = "invalid chunk size [%s]" % str(e)
                return -1
            if size == 0:
                return size
            if size is None or len(rest) < size + 2:
                return None
            body_part, rest = rest[:size], rest[size:]

            # maybe decompress
            body_part = self._decompress(body_part)
            self._partial_body = True
            self._body.append(body_part)
            rest = rest[2:]
            self._buf = [rest] if rest else []
            return len(rest) + 2

    def _parse_chunk_size(self, data):
        idx = data.find(b'\r\n')
        if idx < 0:
            return None, None
        line, rest_chunk = data[:idx], data[idx+2:]
        chunk_size = line.split(b';', 1)[0].strip()
        try:
            chunk_size = int(chunk_size, 16)
        except ValueError:
            raise InvalidChunkSize(chunk_size)
        if chunk_size == 0:
            self._parse_trailers(rest_chunk)
            return 0, None
        return chunk_size, rest_chunk

    def _parse_trailers(self, data):
        idx = data.find(b'\r\n\r\n')
        if data[:2] == b'\r\n':
            self._trailers = self._parse_headers(data[:idx])

    def _decompress(self, data):
        deco = self.__decompress_obj
        if deco is not None:
            if not self.__decompress_first_try:
                data = deco.decompress(data)
            else:
                try:
                    data = deco.decompress(data)
                except zlib.error:
                    self.__decompress_obj = zlib.decompressobj(-zlib.MAX_WBITS)
                    deco = self.__decompress_obj
                    data = deco.decompress(data)
                self.__decompress_first_try = False
        return data


if not hasextensions:   # pragma    nocover
    setDefaultHttpParser(HttpParser)


# ############################################    UTILITIES, ENCODERS, PARSERS
absolute_http_url_re = re.compile(r"^https?://", re.I)


def is_absolute_uri(location):
    '''Check if a ``location`` is absolute, i.e. it includes the scheme
    '''
    return location and absolute_http_url_re.match(location)


def get_environ_proxies():
    """Return a dict of environment proxies. From requests_."""

    proxy_keys = [
        'all',
        'http',
        'https',
        'ftp',
        'socks',
        'ws',
        'wss',
        'no'
    ]

    def get_proxy(k):
        return os.environ.get(k) or os.environ.get(k.upper())

    proxies = [(key, get_proxy(key + '_proxy')) for key in proxy_keys]
    return dict([(key, val) for (key, val) in proxies if val])


def appendslash(url):
    '''Append a slash to *url* if it does not have one.'''
    if not url.endswith('/'):
        url = '%s/' % url
    return url


def choose_boundary():
    """Our embarassingly-simple replacement for mimetools.choose_boundary."""
    return uuid4().hex


def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


def encode_multipart_formdata(fields, boundary=None, charset=None):
    """Encode a dictionary of ``fields`` using the multipart/form-data format.

    :param fields:
        Dictionary of fields or list of (key, value) field tuples.  The key is
        treated as the field name, and the value as the body of the form-data
        bytes. If the value is a tuple of two elements, then the first element
        is treated as the filename of the form-data section.

        Field names and filenames must be unicode.

    :param boundary:
        If not specified, then a random boundary will be generated using
        :func:`mimetools.choose_boundary`.
    """
    charset = charset or 'utf-8'
    body = BytesIO()
    if boundary is None:
        boundary = choose_boundary()
    for fieldname, value in mapping_iterator(fields):
        body.write(('--%s\r\n' % boundary).encode(charset))
        if isinstance(value, tuple):
            filename, data = value
            body.write(('Content-Disposition: form-data; name="%s"; '
                        'filename="%s"\r\n' % (fieldname, filename))
                       .encode(charset))
            body.write(('Content-Type: %s\r\n\r\n' %
                       (get_content_type(filename))).encode(charset))
        else:
            data = value
            body.write(('Content-Disposition: form-data; name="%s"\r\n'
                        % (fieldname)).encode(charset))
            body.write(b'Content-Type: text/plain\r\n\r\n')
        data = body.write(to_bytes(data))
        body.write(b'\r\n')
    body.write(('--%s--\r\n' % (boundary)).encode(charset))
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return body.getvalue(), content_type


def hexmd5(x):
    return md5(to_bytes(x)).hexdigest()


def hexsha1(x):
    return sha1(to_bytes(x)).hexdigest()


def http_date(epoch_seconds=None):
    """
    Formats the time to match the RFC1123 date format as specified by HTTP
    RFC2616 section 3.3.1.

    Accepts a floating point number expressed in seconds since the epoch, in
    UTC - such as that outputted by time.time(). If set to None, defaults to
    the current time.

    Outputs a string in the format 'Wdy, DD Mon YYYY HH:MM:SS GMT'.
    """
    return formatdate(epoch_seconds, usegmt=True)


# ################################################################# COOKIES
def create_cookie(name, value, **kwargs):
    """Make a cookie from underspecified parameters.

    By default, the pair of `name` and `value` will be set for the domain ''
    and sent on every request (this is sometimes called a "supercookie").
    """
    result = dict(
        version=0,
        name=name,
        value=value,
        port=None,
        domain='',
        path='/',
        secure=False,
        expires=None,
        discard=True,
        comment=None,
        comment_url=None,
        rest={'HttpOnly': None},
        rfc2109=False,)
    badargs = set(kwargs) - set(result)
    if badargs:
        err = 'create_cookie() got unexpected keyword arguments: %s'
        raise TypeError(err % list(badargs))
    result.update(kwargs)
    result['port_specified'] = bool(result['port'])
    result['domain_specified'] = bool(result['domain'])
    result['domain_initial_dot'] = result['domain'].startswith('.')
    result['path_specified'] = bool(result['path'])
    return Cookie(**result)


def cookiejar_from_dict(*cookie_dicts):
    """Returns a CookieJar from a key/value dictionary.

    :param cookie_dict: Dict of key/values to insert into CookieJar.
    """
    cookie_dicts = tuple((d for d in cookie_dicts if d))
    if len(cookie_dicts) == 1 and isinstance(cookie_dicts[0], CookieJar):
        return cookie_dicts[0]
    cookiejar = CookieJar()
    for cookie_dict in cookie_dicts:
        if isinstance(cookie_dict, CookieJar):
            for cookie in cookie_dict:
                cookiejar.set_cookie(cookie)
        else:
            for name in cookie_dict:
                cookiejar.set_cookie(create_cookie(name, cookie_dict[name]))
    return cookiejar


# ################################################################# VARY HEADER
cc_delim_re = re.compile(r'\s*,\s*')


def patch_vary_headers(response, newheaders):
    """Adds (or updates) the "Vary" header in the given HttpResponse object.

    newheaders is a list of header names that should be in "Vary". Existing
    headers in "Vary" aren't removed.

    For information on the Vary header, see:

        http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.44
    """
    # Note that we need to keep the original order intact, because cache
    # implementations may rely on the order of the Vary contents in, say,
    # computing an MD5 hash.
    if 'Vary' in response:
        vary_headers = cc_delim_re.split(response['Vary'])
    else:
        vary_headers = []
    # Use .lower() here so we treat headers as case-insensitive.
    existing_headers = set([header.lower() for header in vary_headers])
    additional_headers = [newheader for newheader in newheaders
                          if newheader.lower() not in existing_headers]
    response['Vary'] = ', '.join(vary_headers + additional_headers)


def has_vary_header(response, header_query):
    """
    Checks to see if the response has a given header name in its Vary header.
    """
    if not response.has_header('Vary'):
        return False
    vary_headers = cc_delim_re.split(response['Vary'])
    existing_headers = set([header.lower() for header in vary_headers])
    return header_query.lower() in existing_headers


class CacheControl:
    '''
    http://www.mnot.net/cache_docs/

.. attribute:: maxage

    Specifies the maximum amount of time that a representation will be
    considered fresh.
    '''
    def __init__(self, maxage=None, private=False,
                 must_revalidate=False, proxy_revalidate=False,
                 nostore=False):
        self.maxage = maxage
        self.private = private
        self.must_revalidate = must_revalidate
        self.proxy_revalidate = proxy_revalidate
        self.nostore = nostore

    def __call__(self, headers, etag=None):
        if self.nostore:
            headers['cache-control'] = ('no-store, no-cache, must-revalidate,'
                                        ' max-age=0')
        elif self.maxage:
            headers['cache-control'] = 'max-age=%s' % self.maxage
            if etag:
                headers['etag'] = '"%s"' % etag
            if self.private:
                headers.add_header('cache-control', 'private')
            else:
                headers.add_header('cache-control', 'public')
            if self.must_revalidate:
                headers.add_header('cache-control', 'must-revalidate')
            elif self.proxy_revalidate:
                headers.add_header('cache-control', 'proxy-revalidate')
        else:
            headers['cache-control'] = 'no-cache'


def chunk_encoding(chunk):
    '''Write a chunk::

        chunk-size(hex) CRLF
        chunk-data CRLF

    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    head = ("%X\r\n" % len(chunk)).encode('utf-8')
    return head + chunk + b'\r\n'


def http_chunks(data, finish=False):
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        yield chunk_encoding(chunk)
    if data:
        yield chunk_encoding(data)
    if finish:
        yield chunk_encoding(data)


def parse_header_links(value):
    """Return a dict of parsed link headers proxies

    i.e. Link: <http:/.../front.jpeg>; rel=front; type="image/jpeg",
    <http://.../back.jpeg>; rel=back;type="image/jpeg"

    Original code from https://github.com/kennethreitz/requests

    Copyright 2016 Kenneth Reitz
    """
    links = []
    replace_chars = " '\""

    for val in re.split(", *<", value):
        try:
            url, params = val.split(";", 1)
        except ValueError:
            url, params = val, ''
        link = {}
        link["url"] = url.strip("<> '\"")
        for param in params.split(";"):
            try:
                key, value = param.split("=")
            except ValueError:
                break

            link[key.strip(replace_chars)] = value.strip(replace_chars)
        links.append(link)
    return links
