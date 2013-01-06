'''This is a substantial module which imports several classes and functions
from the standard library in a python 2.6 to python 3.3 compatible fashion.
On top of that, it implements the :class:`HttpClient` for handling synchronous
and asynchronous HTTP requests in a pythonic way.

It is a thin layer on top of urllib2 in python2 / urllib in Python 3.
Several opensource efforts have been used as source of snippets:

* http-parser_
* request_
* urllib3_
* werkzeug_

This is a long stand-alone module which can be dropped in any library and used
as it is.

Usage
===========
Making requests with the :class:`HttpClient` is simple. First you create
a client, which can be as simple as::

    >>> client = HttpClient()

Then you can perform an HTTP request, for example::

    >>> r = client.get('http://www.bbc.co.uk')


.. _http-parser: https://github.com/benoitc/http-parser
.. _urllib3: https://github.com/shazow/urllib3
.. _request: https://github.com/kennethreitz/requests
.. _werkzeug: https://github.com/mitsuhiko/werkzeug
'''
import os
import sys
import re
import io
import string
import time
import json
import mimetypes
import platform
import codecs
import socket
import logging
from functools import reduce
from hashlib import sha1, md5
from base64 import b64encode, b64decode
from uuid import uuid4
from datetime import datetime, timedelta
from email.utils import formatdate
from io import BytesIO
import zlib
from collections import deque, Mapping
from copy import copy

ispy3k = sys.version_info >= (3, 0)
ispy26 = sys.version_info < (2, 7)

create_connection = socket.create_connection
LOGGER = logging.getLogger('httpurl')

try:
    from select import poll, POLLIN
except ImportError: #pragma    nocover
    poll = False
    try:
        from select import select
    except ImportError: #pragma    nocover
        select = False
        
try:    # Compiled with SSL?
    BaseSSLError = None
    ssl = None
    import ssl
    BaseSSLError = ssl.SSLError
except (ImportError, AttributeError):   # pragma : no cover 
    pass

if ispy3k: # Python 3
    from urllib import request as urllibr
    from http import client as httpclient
    from urllib.parse import quote, unquote, urlencode, urlparse, urlsplit,\
                             parse_qs, parse_qsl, splitport, urlunparse,\
                             urljoin
    from http.client import responses
    from http.cookiejar import CookieJar, Cookie
    from http.cookies import SimpleCookie, BaseCookie, Morsel, CookieError

    string_type = str
    getproxies_environment = urllibr.getproxies_environment
    ascii_letters = string.ascii_letters
    zip = zip
    map = map
    range = range
    chr = chr
    iteritems = lambda d : d.items()
    itervalues = lambda d : d.values()
    is_string = lambda s: isinstance(s, str)
    is_string_or_native_string = is_string

    def to_bytes(s, encoding=None, errors=None):
        errors = errors or 'strict'
        encoding = encoding or 'utf-8'
        if isinstance(s, bytes):
            if encoding != 'utf-8':
                return s.decode('utf-8', errors).encode(encoding, errors)
            else:
                return s
        else:
            return ('%s'%s).encode(encoding, errors)

    def to_string(s, encoding=None, errors='strict'):
        """Inverse of to_bytes"""
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8', errors)
        else:
            return str(s)

    def native_str(s, encoding=None):
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8')
        else:
            return s

    def force_native_str(s, encoding=None):
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8')
        elif not isinstance(s, str):
            return str(s)
        else:
            return s

else:   # pragma : no cover
    import urllib2 as urllibr
    import httplib as httpclient
    from urllib import quote, unquote, urlencode, getproxies_environment,\
                        splitport
    from urlparse import urlparse, urlsplit, parse_qs, urlunparse, urljoin,\
                         parse_qsl
    from httplib import responses
    from cookielib import CookieJar, Cookie
    from Cookie import SimpleCookie, BaseCookie, Morsel, CookieError
    from itertools import izip as zip, imap as map

    string_type = unicode
    ascii_letters = string.letters
    range = xrange
    chr = unichr
    iteritems = lambda d : d.iteritems()
    itervalues = lambda d : d.itervalues()
    is_string = lambda s: isinstance(s, unicode)
    is_string_or_native_string = lambda s: isinstance(s, basestring)

    if sys.version_info < (2, 7):
        #
        def create_connection(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                              source_address=None):
            """Form Python 2.7"""
            host, port = address
            err = None
            for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
                af, socktype, proto, canonname, sa = res
                sock = None
                try:
                    sock = socket.socket(af, socktype, proto)
                    if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                        sock.settimeout(timeout)
                    if source_address:
                        sock.bind(source_address)
                    sock.connect(sa)
                    return sock
                except Exception as _:
                    err = _
                    if sock is not None:
                        sock.close()
            if err is not None:
                raise err
            else:
                raise error("getaddrinfo returns an empty list")
    
    def to_bytes(s, encoding=None, errors='strict'):
        encoding = encoding or 'utf-8'
        if isinstance(s, bytes):
            if encoding != 'utf-8':
                return s.decode('utf-8', errors).encode(encoding, errors)
            else:
                return s
        else:
            return unicode(s).encode(encoding, errors)

    def to_string(s, encoding=None, errors='strict'):
        """Inverse of to_bytes"""
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8', errors)
        else:
            return unicode(s)

    def native_str(s, encoding=None):
        if isinstance(s, unicode):
            return s.encode(encoding or 'utf-8')
        else:
            return s

    def force_native_str(s, encoding=None):
        if isinstance(s, unicode):
            return s.encode(encoding or 'utf-8')
        elif not isinstance(s, str):
            return str(s)
        else:
            return s

HTTPError = urllibr.HTTPError
URLError = urllibr.URLError
request_host = urllibr.request_host
parse_http_list = urllibr.parse_http_list

class SSLError(HTTPError):
    "Raised when SSL certificate fails in an HTTPS connection."
    pass

class HTTPurlError(Exception):
    pass

class TooManyRedirects(HTTPurlError):
    pass

def mapping_iterator(iterable):
    if isinstance(iterable, Mapping):
        iterable = iteritems(iterable)
    return iterable

####################################################    URI & IRI SUFF
#
# The reserved URI characters (RFC 3986 - section 2.2)
#Default is charset is "iso-8859-1" (latin-1) from section 3.7.1
#http://www.ietf.org/rfc/rfc2616.txt
DEFAULT_CHARSET = 'iso-8859-1'
URI_GEN_DELIMS = frozenset(':/?#[]@')
URI_SUB_DELIMS = frozenset("!$&'()*+,;=")
URI_RESERVED_SET = URI_GEN_DELIMS.union(URI_SUB_DELIMS)
URI_RESERVED_CHARS = ''.join(URI_RESERVED_SET)
# The unreserved URI characters (RFC 3986 - section 2.3)
URI_UNRESERVED_SET = frozenset(ascii_letters + string.digits + '-._~')
URI_SAFE_CHARS = URI_RESERVED_CHARS + '%~'
HEADER_TOKEN_CHARS = frozenset("!#$%&'*+-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               '^_`abcdefghijklmnopqrstuvwxyz|~')

escape = lambda s: quote(s, safe='~')
urlquote = lambda iri: quote(iri, safe=URI_RESERVED_CHARS)

def _gen_unquote(uri):
    unreserved_set = URI_UNRESERVED_SET
    for n, part in enumerate(force_native_str(uri).split('%')):
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
    '''Convert an Internationalized Resource Identifier (IRI) portion to a URI
portion that is suitable for inclusion in a URL.
This is the algorithm from section 3.1 of RFC 3987.
Returns an ASCII native string containing the encoded result.'''
    if iri is None:
        return iri
    iri = force_native_str(iri)
    if kwargs:
        iri = '%s?%s'%(iri,'&'.join(('%s=%s' % kv for kv in iteritems(kwargs))))
    return urlquote(unquote_unreserved(iri))

def host_and_port(host):
    host, port = splitport(host)
    return host, int(port) if port else None

def host_and_port_default(scheme, host):
    host, port = splitport(host)
    if not port:
        if scheme == "http":
            port = '80'
        elif scheme == "https":
            port = '443'
    return host, port

def remove_double_slash(route):
    if '//' in route:
        route = re.sub('/+', '/', route)
    return route

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
    except:
        return True

####################################################    REQUEST METHODS
ENCODE_URL_METHODS = frozenset(['DELETE', 'GET', 'HEAD', 'OPTIONS'])
ENCODE_BODY_METHODS = frozenset(['PATCH', 'POST', 'PUT', 'TRACE'])
REDIRECT_CODES = (301, 302, 303, 307)

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

####################################################    HTTP HEADERS
WEBSOCKET_VERSION = (8, 13)
HEADER_FIELDS = {'general': frozenset(('Cache-Control', 'Connection', 'Date',
                                       'Pragma', 'Trailer','Transfer-Encoding',
                                       'Upgrade', 'Sec-WebSocket-Extensions',
                                       'Sec-WebSocket-Protocol',
                                       'Via','Warning')),
                 # The request-header fields allow the client to pass additional
                 # information about the request, and about the client itself,
                 # to the server.
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

CLIENT_HEADER_FIELDS = HEADER_FIELDS['general'].union(HEADER_FIELDS['entity'],
                                                      HEADER_FIELDS['request'])
SERVER_HEADER_FIELDS = HEADER_FIELDS['general'].union(HEADER_FIELDS['entity'],
                                                      HEADER_FIELDS['response'])
ALL_HEADER_FIELDS = CLIENT_HEADER_FIELDS.union(SERVER_HEADER_FIELDS)
ALL_HEADER_FIELDS_DICT = dict(((k.lower(),k) for k in ALL_HEADER_FIELDS))

TYPE_HEADER_FIELDS = {'client': CLIENT_HEADER_FIELDS,
                      'server': SERVER_HEADER_FIELDS,
                      'both': ALL_HEADER_FIELDS}

header_type = {0: 'client', 1: 'server', 2: 'both'}
header_type_to_int = dict(((v,k) for k,v in header_type.items()))

def capfirst(x):
    x = x.strip()
    if x:
        return x[0].upper() + x[1:].lower()
    else:
        return x
    
def capheader(name):
    return '-'.join((b for b in (capfirst(n) for n in name.split('-')) if b))

def header_field(name, HEADERS_SET=None, strict=False):
    name = name.lower()
    if name.startswith('x-'):
        return capheader(name)
    else:
        header = ALL_HEADER_FIELDS_DICT.get(name)
        if header and HEADERS_SET:
            return header if header in HEADERS_SET else None
        elif header:
            return header
        elif not strict:
            return capheader(name)

def quote_header_value(value, extra_chars='', allow_token=True):
    """Quote a header value if necessary.

:param value: the value to quote.
:param extra_chars: a list of extra characters to skip quoting.
:param allow_token: if this is enabled token values are returned
                    unchanged."""
    value = force_native_str(value)
    if allow_token:
        token_chars = HEADER_TOKEN_CHARS | set(extra_chars)
        if set(value).issubset(token_chars):
            return value
    return '"%s"' % value.replace('\\', '\\\\').replace('"', '\\"')

def unquote_header_value(value, is_filename=False):
    """Unquotes a header value. Reversal of :func:`quote_header_value`.
This does not use the real unquoting but what browsers are actually
using for quoting.
:param value: the header value to unquote."""
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

class Headers(object):
    '''Utility for managing HTTP headers for both clients and servers.
It has a dictionary like interface
with few extra functions to facilitate the insertion of multiple values.

From http://www.w3.org/Protocols/rfc2616/rfc2616.html

Section 4.2
The order in which header fields with differing field names are received is not
significant. However, it is "good practice" to send general-header fields first,
followed by request-header or response-header fields, and ending with
the entity-header fields.'''
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

    def __iter__(self):
        headers = self._headers
        for k, values in iteritems(headers):
            for value in values:
                yield k, value

    def __len__(self):
        return reduce(lambda x, y: x + len(y), itervalues(self._headers), 0)

    def as_dict(self):
        '''Convert this :class:`Headers` into a dictionary.'''
        return dict(((k, ', '.join(v)) for k, v in iteritems(self._headers)))

    @property
    def kind_number(self):
        return header_type_to_int.get(self.kind)

    def update(self, iterable):
        """Extend the headers with a dictionary or an iterable yielding keys
and values."""
        for key, value in mapping_iterator(iterable):
            self[key] = value

    def __contains__(self, key):
        return header_field(key) in self._headers

    def __getitem__(self, key):
        return ', '.join(self._headers[header_field(key)])

    def __delitem__(self, key):
        self._headers.__delitem__(header_field(key))

    def __setitem__(self, key, value):
        key = header_field(key, self.all_headers, self.strict)
        if key and value is not None:
            if not isinstance(value, list):
                value = [value]
            self._headers[key] = value

    def get(self, key, default=None):
        if key in self:
            return self.__getitem__(key)
        else:
            return default

    def get_all(self, key, default=None):
        return self._headers.get(header_field(key), default)

    def pop(self, key, *args):
        return self._headers.pop(header_field(key), *args)

    def copy(self):
        return self.__class__(self, kind=self.kind)

    def getheaders(self, key):
        '''Required by cookielib. If the key is not available,
it returns an empty list.'''
        return self._headers.get(header_field(key), [])

    def headers(self):
        return list(self)

    def add_header(self, key, value, **params):
        '''Add *value* to *key* header. If the header is already available,
append the value to the list.'''
        key = header_field(key, self.all_headers, self.strict)
        if key and value:
            values = self._headers.get(key, [])
            if value not in values:
                values.append(value)
                self._headers[key] = values

    def flat(self, version, status):
    	'''Full headers bytes representation'''
    	vs = version + (status, self)
    	return ('HTTP/%s.%s %s\r\n%s' % vs).encode(DEFAULT_CHARSET)

    def _ordered(self):
        hf = HEADER_FIELDS
        order = (('general',[]), ('request',[]), ('response',[]), ('entity',[]))
        headers = self._headers
        for key in headers:
            for name, group in order:
                if key in hf[name]:
                    group.append(key)
                    break
        for _, group in order:
            for k in group:
                yield "%s: %s" % (k, ', '.join(headers[k]))
        yield ''
        yield ''


###############################################################################
##    HTTP PARSER
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


class HttpParser(object):
    '''A python HTTP parser.

Original code from https://github.com/benoitc/http-parser

2011 (c) Benoit Chesneau <benoitc@e-engura.org>

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.'''
    def __init__(self, kind=2, decompress=False):
        self.decompress = decompress
        # errors vars
        self.errno = None
        self.errstr = ""
        # protected variables
        self._buf = []
        self._version = None
        self._method = None
        self._server_protocol = None
        self._status_code = None
        self._status = None
        self._reason = None
        self._url = None
        self._path = None
        self._query_string = None
        self._fragment= None
        self._headers = Headers(kind=kind)
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
        self.__decompress_obj = None

    @property
    def kind(self):
        return self._headers.kind_number

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

    def get_protocol(self):
        return self._server_protocol

    def get_body(self):
        return self._body

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
            return self.close(length)
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
                    first_line = native_str(b''.join(self._buf),DEFAULT_CHARSET)
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
                        if to_parse == b'\r\n':
                            self._buf = []
                            return self.close(length)
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

    def close(self, length):
        self.__on_message_begin = True
        self.__on_message_complete = True
        if not self._buf and self.__on_firstline:
            self.__on_headers_complete = True
            return length
        else:
            return length+len(self._buf)
        
    def _parse_firstline(self, line):
        try:
            if self.kind == 2: # auto detect
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
        parts = urlsplit(bits[1])
        self._path = parts.path or ""
        self._query_string = parts.query or ""
        self._fragment = parts.fragment or ""
        self._server_protocol = bits[2]
        # Version
        match = VERSION_RE.match(bits[2])
        if match is None:
            raise InvalidRequestLine("Invalid HTTP version: %s" % bits[2])
        self._version = (int(match.group(1)), int(match.group(2)))

    def _parse_headers(self, data):
        idx = data.find(b'\r\n\r\n')
        if idx < 0: # we don't have all headers
            return False
        chunk = native_str(data[:idx], DEFAULT_CHARSET)
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
            name, value = name.strip(), [value.lstrip()]
            # Consume value continuation lines
            while len(lines) and lines[0].startswith((" ", "\t")):
                value.append(lines.popleft())
            value = ''.join(value).rstrip()
            # multiple headers
            if name in self._headers:
                value = "%s, %s" % (self._headers[name], value)
            # store new header value
            self._headers[name] = value
        # detect now if body is sent by chunks.
        clen = self._headers.get('content-length')
        te = self._headers.get('transfer-encoding', '').lower()
        self._chunked = (te == 'chunked')
        if clen and not self._chunked:
            try:
                clen = int(clen)
            except ValueError:
                clen = None
            else:
                if clen < 0:  # ignore nonsensical negative lengths
                    clen = None
        else:
            clen = None
        status = self._status_code
        if status and (status == httpclient.NO_CONTENT or
            status == httpclient.NOT_MODIFIED or
            100 <= status < 200 or      # 1xx codes
            self._method == "HEAD"):
            clen = 0
        self._clen_rest = self._clen = clen
        # detect encoding and set decompress object
        if self.decompress:
            encoding = self._headers.get('content-encoding')
            if encoding == "gzip":
                self.__decompress_obj = zlib.decompressobj(16+zlib.MAX_WBITS)
            elif encoding == "deflate":
                self.__decompress_obj = zlib.decompressobj()
        rest = data[idx+4:]
        self._buf = [rest]
        self.__on_headers_complete = True
        return len(rest)

    def _parse_body(self):
        data = b''.join(self._buf)
        if not self._chunked:
            if self._clen_rest is not None:
                self._clen_rest -= len(data)
            # maybe decompress
            if self.__decompress_obj is not None:
                data = self.__decompress_obj.decompress(data)
            self._partial_body = True
            if data:
                self._body.append(data)
            self._buf = []
            if self._clen_rest is None or self._clen_rest <= 0:
                self.__on_message_complete = True
        else:
            try:
                size, rest = self._parse_chunk_size(data)
            except InvalidChunkSize as e:
                self.errno = INVALID_CHUNK
                self.errstr = "invalid chunk size [%s]" % str(e)
                return -1
            if size == 0:
                return size
            if size is None or len(rest) < size:
                return None
            body_part, rest = rest[:size], rest[size:]
            if len(rest) < 2:
                self.errno = INVALID_CHUNK
                self.errstr = "chunk missing terminator [%s]" % data
                return -1
            # maybe decompress
            if self.__decompress_obj is not None:
                body_part = self.__decompress_obj.decompress(body_part)
            self._partial_body = True
            self._body.append(body_part)
            self._buf = [rest[2:]]
            return len(rest)

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


################################################################################
##    HTTP CLIENT
################################################################################
class HttpConnectionError(Exception):
    pass


class IOClientRead(object):
    '''Base class for IO clients which keep reading from a socket until a
full parsed message is available.

.. attribute:: async

    True if the socket is asynchronous
'''
    sock = None

    @property
    def async(self):
        return self.sock.gettimeout() == 0

    def parsedata(self, data):
        '''This function is called once data is available on the socket.
If the parser is expecting more data, it should return ``None`` so that
this :class:`IOClientRead` can submit another read request.'''
        raise NotImplementedError()

    def read(self):
        '''Read data from the socket'''
        try:
            return self._read()
        except socket.error:
            self.close()
            raise

    ## INTERNALS
    def _read(self, result=None):
        if not self.sock:
            return
        if self.async:
            r = self.sock.read()
            if not self.sock.closed:
                return r.add_callback(self._got_data, self.close)
            elif r:
                return self.parsedata(r)
            else:
                raise socket.error('Cannot read. Asynchronous socket is closed')
        else:
            # Read from a blocking socket
            length = io.DEFAULT_BUFFER_SIZE
            data = True
            while data:
                data = self.sock.recv(length)
                if not data:
                    # No data. the socket is closed.
                    # We raise socket.error
                    raise socket.error('No data received. Socket is closed')
                else:
                    msg = self.parsedata(data)
                    if msg is not None:
                        return msg
    
    def _got_data(self, data):
        msg = self.parsedata(data)
        if msg is None:
            return self._read()
        else:
            return msg


class HttpParseError(ValueError):
    pass


class HttpResponse(IOClientRead):
    '''An Http response object.

.. attribute:: status_code

    Integer indicating the status code

.. attribute:: history

    A list of :class:`HttpResponse` objects from the history of the
    class:`HttpRequest`. Any redirect responses will end up here.
    
.. attribute:: streaming

    boolean indicating if this is a streaming HTTP response.
'''
    request = None
    parser = None
    will_close = False
    parser_class = HttpParser
    again = False

    def __init__(self, sock, debuglevel=0, method=None, url=None, strict=None):
        self.sock = sock
        self.debuglevel = debuglevel
        self._method = method
        self.strict=strict
        self.__headers = None

    def __str__(self):
        if self.status_code:
            return '%s %s' % (self.status_code, self.response)
        else:
            return '<None>'

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)

    @property
    def streaming(self):
        return self.request.stream
    
    @property
    def status_code(self):
        if self.parser:
            return self.parser.get_status_code()

    @property
    def content(self):
        if self.parser:
            body = self.parser.get_body()
            if body:
                return b''.join(body)

    @property
    def headers(self):
        if self.__headers is None:
            if self.parser and self.parser.is_headers_complete():
                self.__headers = Headers(self.parser.get_headers())
        return self.__headers

    @property
    def is_error(self):
        if self.status_code:
            return not is_succesful(self.status_code)

    @property
    def response(self):
        if self.status_code:
            return responses.get(self.status_code)

    @property
    def url(self):
        if self.request is not None:
           return self.request.full_url
       
    @property
    def history(self):
        if self.request is not None:
           return self.request.history
       
    @property
    def client(self):
        if self.request is not None:
            return self.request.client
        
    @property
    def connection(self):
        if self.request is not None:
            return self.request.connection
        
    def content_string(self, charset=None, errors=None):
        '''Decode content as a string.'''
        data = self.content
        if data is not None:
            return data.decode(charset or 'utf-8', errors or 'strict')

    def content_json(self, charset=None, **kwargs):
        '''Decode content as a JSON object.'''
        return json.loads(self.content_string(charset), **kwargs)

    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`,
 if one occured."""
        if self.is_error:
            raise HTTPError(self.url, self.status_code,
                            self.content, self.headers, None)

    def begin(self, start=False, decompress=True):
        '''Start reading the response. Called by the connection object.'''
        if start and self.parser is None:
            self.parser = self.parser_class(kind=1, decompress=decompress)
            return self.read()
        
    def write(self, data):
        '''Send data to remote server. This can be used in a Exect/continue
situation. Return a deferred if asynchronous.'''
        if self.connection:
            data = to_bytes(data, DEFAULT_CHARSET)
            self.parser = self.parser_class(kind=1,
                                            decompress=self.parser.decompress)
            self.connection.send(data)
            return self._read()
        else:
            raise RuntimeError('No connection. Response closed.')

    def parsedata(self, data):
        '''Called when data is available on the pipeline'''
        had_headers = self.parser.is_headers_complete()
        if self.parser.execute(data, len(data)) == len(data): 
            if self.streaming:
                # if headers are ready, try to close the response
                if had_headers:
                    return self.close()
                elif self.parser.is_headers_complete():
                    res = self.request.client.build_response(self) or self
                    if not res.parser.is_message_complete():
                        res._read()
                    return res
            # Not streaming. wait until we have the whole message
            elif self.parser.is_headers_complete() and\
                 self.parser.is_message_complete():
                return self.request.client.build_response(self)
        else:
            # This is an error in the parsing. Raise an error so that the
            # connection is closed
            raise HttpParseError()

    def isclosed(self):
        '''Required by python HTTPConnection. It is ``True`` when the
:class:`HttpResponse` has received all data (headers and body).'''
        return self.parser.is_message_complete()
    
    def info(self):
        return self.headers

    def stream(self):
        '''Returns a generator of parsed body data. This is available when
the client is in a streaming mode.'''
        if self.streaming:
            if hasattr(self, '_streamed'):
                raise RuntimeError('Already streamed')
            self._streamed = True
            while not self.parser.is_message_complete():
                yield self.parser.recv_body()
            # last part only if boy is available
            b = self.parser.recv_body()
            if b:
                yield b
    
    def close(self):
        if self.parser.is_message_complete():
            # release the connection
            if self.again:
                return self.client.new_request(self)
            else:
                self.client.release_connection(self.request)
            return self
            

class HttpConnection(httpclient.HTTPConnection):
    '''Http Connection class'''
    tunnel_class = httpclient.HTTPResponse
    
    def __init__(self, host, port=None, source_address=None, **kwargs):
        httpclient.HTTPConnection.__init__(self, host, port, **kwargs)
        self.source_address = source_address
    
    def _tunnel(self):
        response_class = self.response_class
        self.response_class = self.tunnel_class
        httpclient.HTTPConnection._tunnel(self)
        self.response_class = response_class

    @property
    def is_async(self):
        return self.sock.gettimeout() == 0 if self.sock else False
    
    @property
    def closed(self):
        return is_closed(self.sock)
    
    def connect(self):
        """Connect to the host and port specified in __init__."""
        self.sock = create_connection((self.host,self.port),
                                       self.timeout, self.source_address)
        if self._tunnel_host:
            self._tunnel()
    
    if ispy26:
        def set_tunnel(self, host, port=None, headers=None):
            return self._set_tunnel(host, port, headers)


class HttpsConnection(HttpConnection):
    '''Https Connection class.'''
    default_port = httpclient.HTTPS_PORT

    def set_cert(self, key_file=None, cert_file=None,
                 cert_reqs='CERT_NONE', ca_certs=None):
        ssl_req_scheme = {
            'CERT_NONE': ssl.CERT_NONE,
            'CERT_OPTIONAL': ssl.CERT_OPTIONAL,
            'CERT_REQUIRED': ssl.CERT_REQUIRED
        }
        self.key_file = key_file
        self.cert_file = cert_file
        self.cert_reqs = ssl_req_scheme.get(cert_reqs) or ssl.CERT_NONE
        self.ca_certs = ca_certs

    def connect(self):
        "Connect to a host on a given (SSL) port."
        HttpConnection.connect(self)
        self.sock = ssl.wrap_socket(self.sock, self.key_file,
                                    self.cert_file,
                                    cert_reqs=self.cert_reqs,
                                    ca_certs=self.ca_certs)
        if self.ca_certs:
            try:
                ssl.match_hostname(self.sock.getpeercert(), self.host)
            except Exception:
                self.sock.shutdown(socket.SHUT_RDWR)
                self.sock.close()
                raise


class HttpBase(object):
    hooks = None
    HOOKS = ('args', 'pre_request', 'pre_send', 'post_request', 'response')

    def register_hook(self, event, hook):
        '''Register an event hook'''
        self.hooks[event].append(hook)

    def dispatch_hook(self, key, hook_data):
        """Dispatches a hook dictionary on a given piece of data."""
        hooks = self.hooks
        if hooks and key in hooks:
            for hook in hooks[key]:
                try:
                    hook(hook_data)
                except Exception:
                    LOGGER.critical('Unhandled error in %s hook' % key,
                                    exc_info=True)

    def add_basic_authentication(self, username, password):
        '''Add a :class:`HTTPBasicAuth` handler to the *pre_requests* hooks.'''
        self.register_hook('pre_request', HTTPBasicAuth(username, password))
        
    def add_digest_authentication(self, username, password):
        self.register_hook('pre_request', HTTPDigestAuth(username, password))

    def has_header(self, key):
        return key in self.headers

    def add_header(self, key, value):
        self.headers.add_header(key, value)

    def get_header(self, key, default=None):
        return self.headers.get(key, default)


class HttpRequest(HttpBase):
    '''Http client request initialised by a call to the
:class:`HttpClient.request` method.

.. attribute:: client

    The :class:`HttpClient` performing the request

.. attribute:: type

    The scheme of the of the URI requested. One of http, https
'''
    response_class = HttpResponse

    _tunnel_host = None
    _has_proxy = False
    def __init__(self, client, url, method, data=None, files=None,
                 charset=None, encode_multipart=True, multipart_boundary=None,
                 headers=None, timeout=None, hooks=None, history=None,
                 allow_redirects=False, max_redirects=10, stream=False):
        self.client = client
        self.type, self.host, self.path, self.params,\
        self.query, self.fragment = urlparse(url)
        self.full_url = self._get_full_url()
        self.timeout = timeout
        self.headers = client.get_headers(self, headers)
        self.hooks = hooks
        self.history = history
        self.max_redirects = max_redirects
        self.allow_redirects = allow_redirects
        self.charset = charset or DEFAULT_CHARSET
        self.method = method.upper()
        self.data = data if data is not None else {}
        self.files = files
        self.stream = stream
        # Pre-request hook.
        self.dispatch_hook('pre_request', self)
        self.encode(encode_multipart, multipart_boundary)

    def execute(self, tried=False):
        '''Submit request and return a :attr:`response_class` instance.'''
        self.connection = connection = self.client.get_connection(self)
        if self._tunnel_host:
            tunnel_headers = {}
            proxy_auth_hdr = "Proxy-Authorization"
            if proxy_auth_hdr in self.headers:
                tunnel_headers[proxy_auth_hdr] = headers[proxy_auth_hdr]
                self.headers.pop(proxy_auth_hdr)
            connection.set_tunnel(self._tunnel_host, headers=tunnel_headers)
        self.dispatch_hook('pre_send', self)
        headers = self.headers.as_dict()
        try:
            connection.request(self.method, self.full_url, self.body, headers)
            response = connection.getresponse()
            response.request = self
            return response.begin(True, self.client.decompress)
        except (socket.error, IOError):
            if tried:
                raise
            self.client.release_connection(self, remove=True)
            return self.execute(True)

    @property
    def selector(self):
        if self.has_proxy():
            return self.full_url
        elif self.query:
            return '%s?%s' % (self.path,self.query)
        else:
            return self.path

    def get_type(self):
        return self.type

    def host_and_port(self):
        return host_and_port(self.host)

    def add_unredirected_header(self, key, value):
        self.headers[header_field(key)] = value

    def set_proxy(self, host, type):
        if self.type == 'https' and not self._tunnel_host:
            self._tunnel_host = self.host
        else:
            self.type = type
            self._has_proxy = True
        self.host = host

    def has_proxy(self):
        return self._has_proxy

    @property
    def key(self):
        host, port = self.host_and_port()
        return (self.type, host, port, self.timeout)

    def is_unverifiable(self):
        return bool(self.history)
    # python 3.3 compatibility
    unverifiable = property(is_unverifiable)

    def encode(self, encode_multipart, multipart_boundary):
        body = None
        if self.method in ENCODE_URL_METHODS:
            self.files = None
            self._encode_url(self.data)
        elif isinstance(self.data, bytes):
            body = self.data
        elif is_string(self.data):
            body = to_bytes(self.data, self.charset)
        elif self.data:
            content_type = self.headers.get('content-type')
            # No content type given
            if not content_type:
                content_type = 'application/x-www-form-urlencoded'
                if encode_multipart:
                    body, content_type = encode_multipart_formdata(
                                                self.data,
                                                boundary=multipart_boundary,
                                                charset=self.charset)
                else:
                    body = urlencode(self.data)
                self.headers['Content-Type'] = content_type
            elif content_type == 'application/json':
                body = json.dumps(self.data).encode('latin-1')
            else:
                body = json.dumps(self.data).encode('latin-1')
        self.body = body

    def _encode_url(self, body):
        query = self.query
        if body:
            if is_string_or_native_string(body):
                body = parse_qsl(body)
            else:
                body = mapping_iterator(body)
            query = parse_qsl(query)
            query.extend(body)
            self.data = query
            query = urlencode(query)
        self.query = query
        self.full_url = self._get_full_url()

    def _get_full_url(self):
        return urlunparse((self.type, self.host, self.path,
                                   self.params, self.query, ''))

    def get_full_url(self):
        '''Needed so that this class is compatible with the Request class
in the http.client module in the standard library.'''
        return self.full_url
    
    def get_origin_req_host(self):
        request = self.history[-1].request if self.history else self
        return request_host(request)


class HttpConnectionPool(object):
    '''Maintains a pool of connections'''
    def __init__(self, client, scheme, host, port, timeout, **params):
        self.client = client
        self.type = scheme
        self.host = host
        self.port = port
        self.timeout = timeout
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self.https_params = params

    def get_connection(self):
        "Get a connection from the pool"
        try:
            closed = True
            while closed:
                connection = self._available_connections.pop()
                closed = connection.closed
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.client.max_connections:
            raise HttpConnectionError("Too many connections")
        self._created_connections += 1
        if self.type == 'https':
            if not ssl:
                raise SSLError("Can't connect to HTTPS URL because the SSL "
                               "module is not available.")
            con = self.client.https_connection(host=self.host, port=self.port)
            con.set_cert(**self.https_params)
        else:
            con = self.client.http_connection(host=self.host, port=self.port)
        if self.timeout is not None:
            con.timeout = self.timeout
        return con

    def release(self, connection):
        "Releases the connection back to the pool"
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

    def remove(self, connection):
        '''Remove the *connection* from the pool'''
        self._in_use_connections.remove(connection)
        try:
            connection.close()
        except:
            pass

    def on_connect(self, connection):
        pass


class HttpClient(HttpBase):
    '''A client for an HTTP/HTTPS server which handles a pool of synchronous
or asynchronous connections.

.. attribute:: headers

    Default headers for this :class:`HttpClient`. If supplied, it must be an
    iterable over two-elements tuple.

    Default: ``None``.

.. attribute:: cookies

    Default cookies for this :class:`HttpClient`

    Default: ``None``.

.. attribute:: timeout

    Default timeout for the connecting sockets. If 0 it is an asynchronous
    client.

.. attribute:: hooks

    Dictionary of event-handling hooks (idea from request_).

.. attribute:: encode_multipart

    Flag indicating if body data is encoded using the ``multipart/form-data``
    encoding by default.

    Default: ``True``

.. attribute:: DEFAULT_HTTP_HEADERS

    Default headers for this :class:`HttpClient`

.. attribute:: proxy_info

    Dictionary of proxy servers for this client.
'''
    timeout = None
    allow_redirects = False
    stream = False
    request_class = HttpRequest
    '''Class handling requests. Default: :class:`HttpRequest`'''
    connection_pool = HttpConnectionPool
    http_connection = HttpConnection
    '''Class handling HTTP connections. Default: :class:`HttpConnection`'''
    https_connection = HttpsConnection
    '''Class handling HTTPS connections. Default: :class:`HttpsConnection`'''
    client_version = 'Python-httpurl'
    DEFAULT_HTTP_HEADERS = Headers([
            ('Connection', 'Keep-Alive'),
            ('Accept-Encoding', 'identity'),
            ('Accept-Encoding', 'deflate'),
            ('Accept-Encoding', 'compress'),
            ('Accept-Encoding', 'gzip')],
            kind='client')
    request_parameters = ('hooks', 'encode_multipart', 'timeout',
                          'max_redirects', 'allow_redirects',
                          'multipart_boundary', 'stream')
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxy_info dictionary
    no_proxy = set(('localhost', urllibr.localhost(), platform.node()))

    def __init__(self, proxy_info=None, timeout=None, cache=None,
                 headers=None, encode_multipart=True, client_version=None,
                 multipart_boundary=None, max_connections=None,
                 key_file=None, cert_file=None, cert_reqs='CERT_NONE',
                 ca_certs=None, cookies=None, trust_env=True, stream=None,
                 store_cookies=True, max_redirects=10, decompress=True):
        self.trust_env = trust_env
        self.store_cookies = store_cookies
        self.max_redirects = max_redirects
        self.poolmap = {}
        self.timeout = timeout if timeout is not None else self.timeout
        self.cookies = cookies
        self.decompress = decompress
        self.stream = stream if stream is not None else stream
        self.max_connections = max_connections or 2**31
        dheaders = self.DEFAULT_HTTP_HEADERS.copy()
        self.client_version = client_version or self.client_version
        dheaders['user-agent'] = self.client_version
        if headers:
            dheaders.update(headers)
        self.hooks = dict(((event, []) for event in self.HOOKS))
        self.headers = dheaders
        self.proxy_info = dict(proxy_info or ())
        if not self.proxy_info and self.trust_env:
            self.proxy_info = get_environ_proxies()
            if 'no' not in self.proxy_info:
                self.proxy_info['no'] = ','.join(self.no_proxy)
        self.encode_multipart = encode_multipart
        self.multipart_boundary = multipart_boundary or choose_boundary()
        self.https_defaults = {'key_file': key_file,
                               'cert_file': cert_file,
                               'cert_reqs': cert_reqs,
                               'ca_certs': ca_certs}

    @property
    def websocket_key(self):
        if not hasattr(self, '_websocket_key'):
            self._websocket_key = native_str(b64encode(os.urandom(16)),
                                             'latin-1')
        return self._websocket_key
            
    def get_headers(self, request, headers=None):
        '''Returns a :class:`Header` obtained from combining
:attr:`headers` with *headers*. It handles websocket requests.'''
        if request.type in ('ws','wss'):
            d = Headers((('Connection', 'Upgrade'),
                         ('Upgrade', 'websocket'),
                         ('Sec-WebSocket-Version', str(max(WEBSOCKET_VERSION))),
                         ('Sec-WebSocket-Key', self.websocket_key),
                         ('user-agent', self.client_version)),
                         kind='client')
        else:
            d = self.headers.copy()
        if headers:
            d.update(headers)
        return d

    def get(self, url, **kwargs):
        '''Sends a GET request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        kwargs.setdefault('allow_redirects', True)
        return self.request('GET', url, **kwargs)

    def options(self, url, **kwargs):
        '''Sends a OPTIONS request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        kwargs.setdefault('allow_redirects', True)
        return self.request('OPTIONS', url, **kwargs)

    def head(self, url, **kwargs):
        '''Sends a HEAD request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        return self.request('HEAD', url, **kwargs)

    def post(self, url, **kwargs):
        '''Sends a POST request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        '''Sends a PUT request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        '''Sends a PATCH request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        return self.request('PATCH', url, **kwargs)

    def delete(self, url, **kwargs):
        '''Sends a DELETE request and returns a :class:`HttpResponse`
object.

:params url: url for the new :class:`HttpRequest` object.
:param \*\*kwargs: Optional arguments for the :meth:`request` method.
'''
        return self.request('DELETE', url, **kwargs)

    def request(self, method, url, cookies=None, **params):
        '''Constructs and sends an :class:`HttpRequest` to a remote server.
It returns an :class:`HttpResponse` object.

:param method: request method for the :class:`HttpRequest`.
:param url: URL for the :class:`HttpRequest`.
:param params: a dictionary which specify all the optional parameters for
the :class:`HttpRequest` constructor.

:rtype: a :class:`HttpResponse` object.
'''
        for parameter in self.request_parameters:
            self._update_parameter(parameter, params)
        request = self.request_class(self, url, method, **params)
        # Set proxy if required
        self.set_proxy(request)
        if self.cookies:
            self.cookies.add_cookie_header(request)
        if cookies:
            if not isinstance(cookies, CookieJar):
                cookies = cookiejar_from_dict(cookies)
            cookies.add_cookie_header(request)
        return request.execute()

    def _set_cookies(self, cookies):
        if cookies:
            if not isinstance(cookies, CookieJar):
                cookies = cookiejar_from_dict(cookies)
        else:
            cookies = CookieJar()
        self._cookies = cookies
    def _get_cookies(self):
        return self._cookies
    cookies = property(_get_cookies, _set_cookies)

    def get_connection(self, request, **kwargs):
        key = request.key
        pool = self.poolmap.get(key)
        if pool is None:
            if request.type == 'https':
                params = self.https_defaults
                if kwargs:
                    params = params.copy()
                    params.update(kwargs)
            else:
                params = kwargs
            pool = self.connection_pool(self, *key, **params)
            self.poolmap[key] = pool
        connection = pool.get_connection()
        connection.response_class = request.response_class
        return connection

    def release_connection(self, request, remove=False):
        '''Release teh connection in *request*'''
        key = request.key
        pool = self.poolmap.get(key)
        if pool:
            if remove:
                pool.remove(request.connection)
            else:
                pool.release(request.connection)
        request.connection = None

    def upgrade(self, response):
        '''Upgrade a :class:`HttpResponse` (for websocket).
Requires implementation.'''
        raise NotImplementedError()
    
    def set_proxy(self, request):
        if request.type in self.proxy_info:
            hostonly, _ = splitport(request.host)
            no_proxy = [n for n in self.proxy_info.get('no','').split(',') if n]
            if not any(map(hostonly.endswith, no_proxy)):
                p = urlparse(self.proxy_info[request.type])
                request.set_proxy(p.netloc, p.scheme)

    def build_response(self, response):
        '''The response headers are available. Build the response.'''
        request = response.request
        request.dispatch_hook('response', response)
        headers = response.headers
        # store cookies in clinet if needed
        if self.store_cookies and 'set-cookie' in headers:
            self.cookies.extract_cookies(response, request)
        # check redirect
        if response.status_code in REDIRECT_CODES and 'location' in headers and\
                request.allow_redirects:
            url = response.headers.get('location')
            # Handle redirection without scheme (see: RFC 1808 Section 4)
            if url.startswith('//'):
                parsed_rurl = urlparse(request.full_url)
                url = '%s:%s' % (parsed_rurl.scheme, url)
            # Facilitate non-RFC2616-compliant 'location' headers
            # (e.g. '/path/to/resource' instead of
            # 'http://domain.tld/path/to/resource')
            if not urlparse(url).netloc:
                url = urljoin(request.full_url,
                              # Compliant with RFC3986, we percent
                              # encode the url.
                              requote_uri(url))
            return self.new_request(response, url)
        elif response.status_code == 100:
            # Continue with current response
            return response
        elif response.status_code == 101:
                # Upgrading response handler
                return self.upgrade(response)
        else:
            # We need to read the rest of the message
            return response.close()
        
    def new_request(self, response, url=None):
        request = response.request
        url = url or request.full_url
        history = request.history or []
        if len(history) >= request.max_redirects:
            raise TooManyRedirects()
        history.append(response)
        self.release_connection(request)
        data = request.body
        files = request.files
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.3.4
        if response.status_code is 303:
            method = 'GET'
            data = None
            files = None
        else:
            method = request.method
        headers = request.headers
        headers.pop('Cookie', None)
        # Build a new request
        return self.request(method, url, data=data, files=files,
                            headers=headers,
                            encode_multipart=self.encode_multipart,
                            history=history,
                            max_redirects=request.max_redirects,
                            allow_redirects=request.allow_redirects)
        
        
    def _update_parameter(self, name, params):
        if name not in params:
            params[name] = getattr(self, name)
        elif name == 'hooks':
            hooks = params[name]
            chooks = dict(((e, copy(h)) for e, h in iteritems(self.hooks)))
            for e, h in iteritems(hooks):
                chooks[e].append(h)
            params[name] = chooks


###############################################    AUTHENTICATION
class Auth(object):
    """Base class that all auth implementations derive from"""
    type = None
    def __call__(self, r):
        raise NotImplementedError('Auth hooks must be callable.')

    def authenticated(self, *args, **kwargs):
        return False

    def __str__(self):
        return self.__repr__()


class HTTPBasicAuth(Auth):
    """Attaches HTTP Basic Authentication to the given Request object."""
    def __init__(self, username, password):
        self.username = username
        self.password = password
        
    @property
    def type(self):
        return 'basic'

    def __call__(self, request):
        request.headers['Authorization'] = basic_auth_str(self.username,
                                                          self.password)

    def authenticated(self, environ, username, password):
        return username==self.username and password==self.password

    def __repr__(self):
        return 'Basic: %s' % self.username


class HTTPDigestAuth(Auth):
    """Attaches HTTP Digest Authentication to the given Request object."""
    def __init__(self, username, password=None, options=None):
        self.username = username
        self.password = password
        self.last_nonce = None
        self.options = options or {}
        self.algorithm = self.options.pop('algorithm', 'MD5')

    @property
    def type(self):
        return 'digest'
    
    def __call__(self, request):
        # If we have a saved nonce, skip the 401
        if self.last_nonce:
            request.headers['Authorization'] =\
                self.encode(request.method, request.full_url)
        request.register_hook('response', self.handle_401)
        return request

    def __repr__(self):
        return 'Digest: %s' % self.username

    def authenticated(self, environ, username, password):
        '''Called by the server to check if client is authenticated.'''
        if username != self.username:
            return False
        o = self.options
        qop = o.get('qop')
        method = environ['REQUEST_METHOD']
        uri = environ.get('PATH_INFO','')
        ha1 = self.ha1(o['realm'], password)
        ha2 = self.ha2(qop, method, uri)
        if qop is None:
            response = hexmd5(":".join((ha1, self.nonce, ha2)))
        elif qop == 'auth' or qop == 'auth-int':
            response = hexmd5(":".join((ha1, o['nonce'], o['nc'],
                                        o['cnonce'], qop, ha2)))
        else:
            raise ValueError("qop value are wrong")
        return o['response'] == response

    def encode(self, method, uri):
        '''Called by the client to encode Authentication header.'''
        if not self.username or not self.password:
            return
        o = self.options
        qop = o.get('qop')
        realm = o.get('realm')
        nonce = o['nonce']
        entdig = None
        p_parsed = urlparse(uri)
        path = p_parsed.path
        if p_parsed.query:
            path += '?' + p_parsed.query
        KD = lambda s, d: self.hex("%s:%s" % (s, d))
        ha1 = self.ha1(realm, self.password)
        ha2 = self.ha2(qop, method, path)
        if qop == 'auth':
            if nonce == self.last_nonce:
                self.nonce_count += 1
            else:
                self.nonce_count = 1
            ncvalue = '%08x' % self.nonce_count
            s = str(self.nonce_count).encode('utf-8')
            s += nonce.encode('utf-8')
            s += time.ctime().encode('utf-8')
            s += os.urandom(8)
            cnonce = sha1(s).hexdigest()[:16]
            noncebit = "%s:%s:%s:%s:%s" % (nonce, ncvalue, cnonce, qop, ha2)
            respdig = KD(ha1, noncebit)
        elif qop is None:
            respdig = KD(ha1, "%s:%s" % (nonce, ha2))
        else:
            # XXX handle auth-int.
            return
        base = 'username="%s", realm="%s", nonce="%s", uri="%s", ' \
               'response="%s"' % (self.username, realm, nonce, path, respdig)
        opaque = o.get('opaque')
        if opaque:
            base += ', opaque="%s"' % opaque
        if entdig:
            base += ', digest="%s"' % entdig
            base += ', algorithm="%s"' % self.algorithm
        if qop:
            base += ', qop=%s, nc=%s, cnonce="%s"' % (qop, ncvalue, cnonce)
        return 'Digest %s' % (base)
        
    def handle_401(self, response):
        """Takes the given response and tries digest-auth, if needed."""
        request = response.request
        num_401_calls = request.hooks['response'].count(self.handle_401)
        s_auth = response.headers.get('www-authenticate', '')
        if 'digest' in s_auth.lower() and num_401_calls < 2:
            self.options = parse_dict_header(s_auth.replace('Digest ', ''))
            request.headers['Authorization'] = self.encode(request.method,
                                                           request.full_url)
            response.again = True
        return response
    
    def hex(self, x):
        if self.algorithm == 'MD5':
            return hexmd5(x)
        elif self.algorithm == 'SHA1':
            return hexsha1(x)
        else:
            raise valueError('Unknown algorithm %s' % self.algorithm)
        
    def ha1(self, realm, password):
        return self.hex('%s:%s:%s' % (self.username, realm, password))
    
    def ha2(self, qop, method, uri, body=None):
        if qop == "auth" or qop is None:
            return self.hex("%s:%s" % (method, uri))
        elif qop == "auth-int":
            return self.hex("%s:%s:%s" % (method, uri, self.hex(body)))
        raise ValueError()


class WWWAuthenticate(object):
    _require_quoting = frozenset(['domain', 'nonce', 'opaque', 'realm'])
    
    def __init__(self, type, **options):
        self.type = type
        self.options = options
    
    @classmethod
    def basic(cls, realm='authentication required'):
        """Clear the auth info and enable basic auth."""
        return cls('basic', realm=realm)
    
    @classmethod    
    def digest(cls, realm, nonce, qop=('auth',), opaque=None,
               algorithm=None, stale=None):
        options = {}
        if stale:
            options['stale'] = 'TRUE'
        if opaque is not None:
            options['opaque'] = opaque
        if algorithm is not None:
            options['algorithm'] = algorithm
        return cls('digest', realm=realm, nonce=nonce,
                   qop=', '.join(qop), **options)
    
    def __str__(self):
        """Convert the stored values into a WWW-Authenticate header."""
        return '%s %s' % (self.type.title(), ', '.join((
            '%s=%s' % (key, quote_header_value(value,
                                allow_token=key not in self._require_quoting))
            for key, value in iteritems(self.options)
        )))
    __repr__ = __str__
        
###############################################    UTILITIES, ENCODERS, PARSERS
def get_environ_proxies():
    """Return a dict of environment proxies. From requests_."""

    proxy_keys = [
        'all',
        'http',
        'https',
        'ftp',
        'socks',
        'no'
    ]

    get_proxy = lambda k: os.environ.get(k) or os.environ.get(k.upper())
    proxies = [(key, get_proxy(key + '_proxy')) for key in proxy_keys]
    return dict([(key, val) for (key, val) in proxies if val])

def addslash(url):
    '''Add a slash at the beginning of *url*.'''
    if not url.startswith('/'):
        url = '/%s' % url
    return url

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
    """
    Encode a dictionary of ``fields`` using the multipart/form-data mime format.

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
    charset = charset or DEFAULT_CHARSET
    body = BytesIO()
    if boundary is None:
        boundary = choose_boundary()
    for fieldname, value in mapping_iterator(fields):
        body.write(('--%s\r\n' % boundary).encode(charset))
        if isinstance(value, tuple):
            filename, data = value
            body.write(('Content-Disposition: form-data; name="%s"; '
                        'filename="%s"\r\n' % (fieldname, filename))\
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

def basic_auth_str(username, password):
    """Returns a Basic Auth string."""
    b64 = b64encode(('%s:%s' % (username, password)).encode('latin1'))
    return 'Basic %s' % native_str(b64.strip(), 'latin1')
    

digest_parameters=frozenset(('username', 'realm', 'nonce', 'uri', 'nc',
                             'cnonce',  'response'))
def parse_authorization_header(value, charset='utf-8'):
    """Parse an HTTP basic/digest authorization header transmitted by the web
browser.  The return value is either `None` if the header was invalid or
not given, otherwise an :class:`Auth` object.

:param value: the authorization header to parse.
:return: a :class:`Auth` or `None`."""
    if not value:
        return
    try:
        auth_type, auth_info = value.split(None, 1)
        auth_type = auth_type.lower()
    except ValueError:
        return
    if auth_type == 'basic':
        try:
            up = b64decode(auth_info.encode('latin-1')).decode(charset)
            username, password = up.split(':', 1)
        except Exception as e:
            return
        return HTTPBasicAuth(username, password)
    elif auth_type == 'digest':
        auth_map = parse_dict_header(auth_info)
        if not digest_parameters.difference(auth_map):
            return HTTPDigestAuth(auth_map.pop('username'),
                                  options=auth_map)

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

#################################################################### COOKIE
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

def cookiejar_from_dict(cookie_dict, cookiejar=None):
    """Returns a CookieJar from a key/value dictionary.

    :param cookie_dict: Dict of key/values to insert into CookieJar.
    """
    if cookiejar is None:
        cookiejar = CookieJar()
    if cookie_dict is not None:
        for name in cookie_dict:
            cookiejar.set_cookie(create_cookie(name, cookie_dict[name]))
    return cookiejar

def parse_cookie(cookie):
    if cookie == '':
        return {}
    if not isinstance(cookie, BaseCookie):
        try:
            c = SimpleCookie()
            c.load(cookie)
        except CookieError:
            # Invalid cookie
            return {}
    else:
        c = cookie
    cookiedict = {}
    for key in c.keys():
        cookiedict[key] = c.get(key).value
    return cookiedict


cc_delim_re = re.compile(r'\s*,\s*')


class accept_content_type(object):

    def __init__(self, values=None):
        self._all = {}
        self.update(values)

    def update(self, values):
        if values:
            all = self._all
            accept_headers = cc_delim_re.split(values)
            for h in accept_headers:
                v = h.split('/')
                if len(v) == 2:
                    a, b = v
                    if a in all:
                        all[a].append(b)
                    else:
                        all[a] = [b]

    def __contains__(self, content_type):
        all = self._all
        if not all:
            # If no Accept header field is present, then it is assumed that the
            # client accepts all media types.
            return True
        a, b = content_type.split('/')
        if a in all:
            all = all[a]
            if '*' in all:
                return True
            else:
                return b in all
        elif '*' in all:
            return True
        else:
            return False


def patch_vary_headers(response, newheaders):
    """\
Adds (or updates) the "Vary" header in the given HttpResponse object.
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


class CacheControl(object):
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

    def __call__(self, headers):
        if self.nostore:
            headers['cache-control'] = 'no-store'
        elif self.maxage:
            headers['cache-control'] = 'max-age=%s' % self.maxage
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

