'''This MASSIVE module imports several classes and functions form the standard
library in a python 2.6 to python 3.3 compatible fashion.
On top of that, it implements the :class:`HttpClient` for handling synchronous
and asynchronous HTTP requests in a pythonic way.

It is a thin layer on top of urllib2 in python2 / urllib in Python 3.
Several opensource efforts have been used as source of snippes, inspiration
and more:

* http-parser_
* urllib3_
* request_

This is a long stand-alone module which can be dropped in any library and used
as it is.

Usage
===========
making requests with the :class:`HttpClient` is simple. First you create
a client, which can be as simple as::

    >>> client = HttpClient()

Then you can request a webpage, for example::

    >>> r = client.get('http://www.bbc.co.uk')


.. _http-parser: https://github.com/benoitc/http-parser
.. _urllib3: https://github.com/shazow/urllib3
.. _request: https://github.com/kennethreitz/requests
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
import traceback
from base64 import b64encode, b64decode
from uuid import uuid4
from datetime import datetime, timedelta
from email.utils import formatdate
from io import BytesIO
import zlib
from collections import deque, Mapping
from copy import copy

ispy3k = sys.version_info >= (3, 0)

try:    # Compiled with SSL?
    HTTPSConnection = object
    BaseSSLError = None
    ssl = None
    if ispy3k:
        from http.client import HTTPSConnection
    else:
        from httplib import HTTPSConnection
    import ssl
    BaseSSLError = ssl.SSLError
except (ImportError, AttributeError):
    pass

if ispy3k: # Python 3
    from urllib import request as urllibr
    from http import client as httpclient
    from urllib.parse import quote, unquote, urlencode, urlparse, urlsplit,\
                             parse_qs, parse_qsl, splitport, urlunparse, urljoin
    from http.client import responses
    from http.cookiejar import CookieJar
    from http.cookies import SimpleCookie, BaseCookie, Morsel, CookieError
    from functools import reduce

    string_type = str
    getproxies_environment = urllibr.getproxies_environment
    ascii_letters = string.ascii_letters
    zip = zip
    map = map
    range = range
    iteritems = lambda d : d.items()
    itervalues = lambda d : d.values()
    is_string = lambda s: isinstance(s, str)
    is_string_or_native_string = is_string

    def to_bytes(s, encoding=None, errors='strict'):
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
        else:
            return '%s' % s
        
    def execfile(filename, globals=None, locals=None):
        if globals is None:
            globals = sys._getframe(1).f_globals
        if locals is None:
            locals = sys._getframe(1).f_locals
        with open(filename, "r") as fh:
            exec(fh.read()+"\n", globals, locals)

else:   # pragma : no cover
    import urllib2 as urllibr
    import httplib as httpclient
    from urllib import quote, unquote, urlencode, getproxies_environment,\
                        splitport
    from urlparse import urlparse, urlsplit, parse_qs, urlunparse, urljoin,\
                         parse_qsl
    from httplib import responses
    from cookielib import CookieJar
    from Cookie import SimpleCookie, BaseCookie, Morsel, CookieError
    from itertools import izip as zip, imap as map

    string_type = unicode
    ascii_letters = string.letters
    range = xrange
    iteritems = lambda d : d.iteritems()
    itervalues = lambda d : d.itervalues()
    is_string = lambda s: isinstance(s, unicode)
    is_string_or_native_string = lambda s: isinstance(s, basestring)

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
        else:
            return '%s' % s

HTTPError = urllibr.HTTPError
URLError = urllibr.URLError
request_host = urllibr.request_host

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
URI_GEN_DELIMS = frozenset(':/?#[]@')
URI_SUB_DELIMS = frozenset("!$&'()*+,;=")
URI_RESERVED_SET = URI_GEN_DELIMS.union(URI_SUB_DELIMS)
URI_RESERVED_CHARS = ''.join(URI_RESERVED_SET)
# The unreserved URI characters (RFC 3986 - section 2.3)
URI_UNRESERVED_SET = frozenset(ascii_letters + string.digits + '-._~')

escape = lambda s: quote(s, safe='~')
urlquote = lambda iri: quote(iri, safe=URI_RESERVED_CHARS)

def unquote_unreserved(uri):
    """Un-escape any percent-escape sequences in a URI that are unreserved
characters. This leaves all reserved, illegal and non-ASCII bytes encoded."""
    unreserved_set = URI_UNRESERVED_SET
    parts = to_string(uri).split('%')
    for i in range(1, len(parts)):
        h = parts[i][0:2]
        if len(h) == 2:
            c = chr(int(h, 16))
            if c in unreserved_set:
                parts[i] = c + parts[i][2:]
            else:
                parts[i] = '%' + parts[i]
        else:
            parts[i] = '%' + parts[i]
    return ''.join(parts)

def requote_uri(uri):
    """Re-quote the given URI.

    This function passes the given URI through an unquote/quote cycle to
    ensure that it is fully and consistently quoted.
    """
    # Unquote only the unreserved characters
    # Then quote only illegal characters (do not quote reserved, unreserved,
    # or '%')
    return quote(unquote_unreserved(uri), safe="!#$%&'()*+,/:;=?@[]~")

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
    
def header_field(name, HEADERS_SET=None):
    name = name.lower()
    if name.startswith('x-'):
        return '-'.join((b for b in (capfirst(n) for n in name.split('-')) if b))
    else:
        header = ALL_HEADER_FIELDS_DICT.get(name)
        if header and HEADERS_SET:
            return header if header in HEADERS_SET else None
        else:
            return header


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
    def __init__(self, headers=None, kind='server'):
        if isinstance(kind, int):
            kind = header_type.get(kind, 'both')
        else:
            kind = kind.lower()
        self.kind = kind
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
        return str(self).encode('iso-8859-1')

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

    def update(self, iterable, append=False):
        """Extend the headers with a dictionary or an iterable yielding keys
and values. If append is set to ``True``, existing headers are appended
rather than replaced."""
        iterable = mapping_iterator(iterable)
        if append:        
            add = self.add_header
            for key, value in iterable:
                add(key, value)
        else:
            for key, value in iterable:
                self[key] = value

    def __contains__(self, key):
        return header_field(key) in self._headers

    def __getitem__(self, key):
        return ', '.join(self._headers[header_field(key)])

    def __delitem__(self, key):
        self._headers.__delitem__(header_field(key))

    def __setitem__(self, key, value):
        key = header_field(key, self.all_headers)
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
        key = header_field(key, self.all_headers)
        if key and value:
            values = self._headers.get(key, [])
            if value not in values:
                values.append(value)
                self._headers[key] = values
        #else:
        #    self._headers.pop(key, None)

    def flat(self, version, status):
    	'''Full headers bytes representation'''
    	vs = version + (status, self)
    	return ('HTTP/%s.%s %s\r\n%s' % vs).encode('iso-8859-1')

    @property
    def vary_headers(self):
        return self.get('vary',[])

    def has_vary(self, header_query):
        """Checks to see if the has a given header name in its Vary header.
        """
        return header_query.lower() in set(self.vary_headers)

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
    '''A python http parser.

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

    def is_upgrade(self):
        """ Do we get upgrade header in the request. Useful for
        websockets """
        return self._headers.get('connection') == "upgrade"

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
                    first_line = native_str(b''.join(self._buf), 'iso-8859-1')
                    nb_parsed = nb_parsed + idx + 2

                    rest = data[idx+2:]
                    data = b''
                    if self._parse_firstline(first_line):
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
                if not self.__on_message_begin:
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
        chunk = native_str(data[:idx], 'iso-8859-1')
        # Split lines on \r\n keeping the \r\n on each line
        lines = deque(('%s\r\n' % line for line in chunk.split('\r\n')))

        # Parse headers into key/value pairs paying attention
        # to continuation lines.
        while len(lines):
            # Parse initial header name : value pair.
            curr = lines.popleft()
            if curr.find(":") < 0:
                raise InvalidHeader("invalid line %s" % curr.strip())
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
                value = "%s,%s" % (self._headers[name], value)

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
        '''Read data from socket'''
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


class HttpResponse(IOClientRead):
    '''An Http response object.

.. attribute:: status_code

    Integer indicating the status code

.. attribute:: history

    A list of :class:`HttpResponse` objects from the history of the
    class:`HttpRequest`. Any redirect responses will end up here.
'''
    request = None
    parser = None
    will_close = False
    parser_class = HttpParser

    def __init__(self, sock, debuglevel=0, method=None, url=None,
                 strict=None):
        self.sock = sock
        self.debuglevel = debuglevel
        self._method = method
        self.strict=strict

    def __str__(self):
        if self.status_code:
            return '{0} {1}'.format(self.status_code,self.response)
        else:
            return '<None>'

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)

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
        if self.parser:
            return self.parser.get_headers()

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
        
    def content_string(self, charset=None):
        '''Decode content as a string.'''
        data = self.content
        if data is not None:
            return data.decode(charset or 'utf-8')

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

    def parsedata(self, data):
        self.parser.execute(data, len(data))
        if self.parser.is_message_complete():
            return self.request.client.build_response(self)

    def close(self):
        self.sock.close()

    def info(self):
        return self.headers


class HttpConnection(httpclient.HTTPConnection):
    '''Http Connection class'''
    tunnel_class = httpclient.HTTPResponse
    def _tunnel(self):
        response_class = self.response_class
        self.response_class = self.tunnel_class
        httpclient.HTTPConnection._tunnel(self)
        self.response_class = response_class

    @property
    def is_async(self):
        return self.sock.gettimeout() == 0


class HttpsConnection(HTTPSConnection):
    '''Https Connection class.'''
    tunnel_class = httpclient.HTTPResponse
    def _tunnel(self):
        response_class = self.response_class
        self.response_class = self.tunnel_class
        HTTPSConnection._tunnel(self)
        self.response_class = response_class

    @property
    def is_async(self):
        return self.sock.gettimeout() == 0
    
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

    def __connect(self):
        # Add certificate verification
        sock = socket.create_connection((self.host, self.port), self.timeout)
        # Wrap socket using verification with the root certs in
        # trusted_root_certs
        self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file,
                                    cert_reqs=self.cert_reqs,
                                    ca_certs=self.ca_certs)
        if self.ca_certs:
            match_hostname(self.sock.getpeercert(), self.host)


class HttpBase(object):
    HOOKS = ('args', 'pre_request', 'pre_send', 'post_request', 'response')

    def register_hook(self, event, hook):
        '''Register an event hook'''
        self.hooks[event].append(hook)

    def dispatch_hook(self, key, hooks, hook_data):
        """Dispatches a hook dictionary on a given piece of data."""
        if hooks and key in hooks:
            for hook in hooks[key]:
                try:
                    hook(hook_data)
                except Exception:
                    traceback.print_exc()

    def add_basic_authentication(self, username, password):
        '''Add a :class:`HTTPBasicAuth` handler to the *pre_requests* hooks.'''
        self.register_hook('pre_request', HTTPBasicAuth(username, password))

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
    default_charset = 'latin-1'
    '''Default is charset is "iso-8859-1" (latin-1) from section 3.7.1
http://www.ietf.org/rfc/rfc2616.txt
    '''

    _tunnel_host = None
    _has_proxy = False
    def __init__(self, client, url, method, data=None, files=None,
                 charset=None, encode_multipart=True, multipart_boundary=None,
                 headers=None, timeout=None, hooks=None, history=None,
                 allow_redirects=False, max_redirects=10, **kwargs):
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
        self.charset = charset or self.default_charset
        self.method = method.upper()
        self.data = data if data is not None else {}
        self.files = files
        # Pre-request hook.
        self.dispatch_hook('pre_request', self.hooks, self)
        self.encode(encode_multipart, multipart_boundary)

    def get_response(self):
        '''Submit request and return a :attr:`response_class` instance.'''
        self.connection = connection = self.client.get_connection(self)
        if self._tunnel_host:
            tunnel_headers = {}
            proxy_auth_hdr = "Proxy-Authorization"
            if proxy_auth_hdr in self.headers:
                tunnel_headers[proxy_auth_hdr] = headers[proxy_auth_hdr]
                self.headers.pop(proxy_auth_hdr)
            connection.set_tunnel(self._tunnel_host, headers=tunnel_headers)
        self.dispatch_hook('pre_send', self.hooks, self)
        headers = self.headers.as_dict()
        connection.request(self.method, self.full_url, self.body, headers)
        response = connection.getresponse()
        response.request = self
        return response.begin(True, self.client.decompress)

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
        # unverifiable == redirected
        return bool(self.history)

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
        if isinstance(body, dict):
            if query:
                query = parse_qs(query)
                query.update(body)
            else:
                query = body
            query = urlencode(query)
        elif body:
            if query:
                query = parse_qs(query)
                query.update(parse_qs(body))
                query = urlencode(query)
            else:
                query = body
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

    def on_response(self, response):
        return response


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
            connection = self._available_connections.pop()
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
            con = HttpsConnection(host=self.host, port=self.port)
            con.set_cert(**self.https_params)
        else:
            con = HttpConnection(host=self.host, port=self.port)
        if self.timeout is not None:
            con.timeout = self.timeout
        return con

    def release(self, connection):
        "Releases the connection back to the pool"
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

    def remove(self, connection):
        self._in_use_connections.remove(connection)


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

    Dictionary of proxy servers for this client

The :attr:`key_file``, :attr:`cert_file`, :attr:`cert_reqs` and
:attr:`ca_certs` parameters are only used if :mod:`ssl` is available
and are fed into :meth:`ssl.wrap_socket` to upgrade the connection socket
into an SSL socket.
'''
    timeout = None
    request_class = HttpRequest
    connection_pool = HttpConnectionPool
    client_version = 'Python-httpurl'
    DEFAULT_HTTP_HEADERS = Headers([
            ('Connection', 'Keep-Alive'),
            ('Accept-Encoding', 'identity'),
            ('Accept-Encoding', 'deflate'),
            ('Accept-Encoding', 'compress'),
            ('Accept-Encoding', 'gzip')],
            kind='client')
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxy_info dictionary
    no_proxy = set(('localhost', urllibr.localhost(), platform.node()))

    def __init__(self, proxy_info=None, timeout=None, cache=None,
                 headers=None, encode_multipart=True, client_version=None,
                 multipart_boundary=None, max_connections=None,
                 key_file=None, cert_file=None, cert_reqs='CERT_NONE',
                 ca_certs=None, cookies=None, trust_env=True,
                 store_cookies=True, max_redirects=10, decompress=True):
        self.trust_env = trust_env
        self.store_cookies = store_cookies
        self.max_redirects = max_redirects
        self.poolmap = {}
        self.timeout = timeout if timeout is not None else self.timeout
        self.cookies = cookies
        self.decompress = decompress
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
:attr:`headers` with *headers*.'''
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

    def path(self, url, **kwargs):
        '''Sends a PATH request and returns a :class:`HttpResponse`
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

    def request(self, method, url, data=None, files=None, headers=None,
                encode_multipart=None, allow_redirects=False, hooks=None,
                cookies=None, history=None, max_redirects=None, **kwargs):
        '''Constructs, sends a :class:`HttpRequest` and returns
a :class:`HttpResponse` object.

:param url: URL for the new :class:`HttpRequest`.
:param data: optional dictionary or bytes to be sent either in the query string
    for 'DELETE', 'GET', 'HEAD' and 'OPTIONS' methods or in the body
    for 'PATCH', 'POST', 'PUT', 'TRACE' methods.
:param method: optional request method for the :class:`HttpRequest`.
:param hooks:
'''
        if hooks:
            chooks = dict(((e, copy(h)) for e, h in iteritems(self.hooks)))
            for e, h in iteritems(hooks):
                chooks[e].append(h)
            hooks = chooks
        else:
            hooks = self.hooks
        encode_multipart = encode_multipart if encode_multipart is not None\
                            else self.encode_multipart
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        if max_redirects is None:
            max_redirects = self.max_redirects
        request = self.request_class(self, url, method, data=data, files=files,
                                     headers=headers,
                                     encode_multipart=encode_multipart,
                                     multipart_boundary=self.multipart_boundary,
                                     hooks=hooks,
                                     history=history,
                                     max_redirects=max_redirects,
                                     allow_redirects=allow_redirects,
                                     **kwargs)
        # Set proxy if required
        self.set_proxy(request)
        if self.cookies:
            self.cookies.add_cookie_header(request)
        if cookies:
            if not isinstance(cookies, CookieJar):
                cookies = cookiejar_from_dict(cookies)
            cookies.add_cookie_header(request)
        return request.get_response()

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

    def release_connection(self, req):
        key = req.key
        pool = self.poolmap.get(key)
        if pool:
            pool.remove(req.connection)

    def set_proxy(self, request):
        if request.type in self.proxy_info:
            hostonly, _ = splitport(request.host)
            no_proxy = [n for n in self.proxy_info.get('no','').split(',') if n]
            if not any(map(hostonly.endswith, no_proxy)):
                p = urlparse(self.proxy_info[request.type])
                request.set_proxy(p.netloc, p.scheme)

    def build_response(self, response):
        request = response.request
        headers = response.headers
        if self.store_cookies:
            #response.cookies = CookieJar()
            if 'set-cookie' in headers:
                #response.cookies.extract_cookies(response, request)
                self.cookies.extract_cookies(response, request)
        if response.status_code in REDIRECT_CODES and 'location' in headers and\
                request.allow_redirects:
            history = request.history or []
            if len(history) >= request.max_redirects:
                raise TooManyRedirects()
            history.append(response)
            self.release_connection(request)
            url = response.headers['location']
            data = request.body
            files = request.files
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
        else:
            return request.on_response(response)


###############################################    AUTH
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

    def authenticated(self, username, password):
        return username==self.username and password==self.password

    def __repr__(self):
        return 'Basic: %s' % self.username


class HTTPDigestAuth(Auth):
    """Attaches HTTP Digest Authentication to the given Request object."""
    def __init__(self, username, password):
        self.username = username
        self.password = password

    @property
    def type(self):
        return 'digest'

    def __call__(self, request):
        request.register_hook('response', self.handle_401)

    def __repr__(self):
        return 'Digest: %' % self.username

    def handle_401(self):
        pass


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
    charset = charset or Request.default_charset
    body = BytesIO()
    if boundary is None:
        boundary = choose_boundary()

    if isinstance(fields, dict):
        fields = iteritems(fields)

    for fieldname, value in fields:
        body.write(('--%s\r\n' % boundary).encode(charset))

        if isinstance(value, tuple):
            filename, data = value
            body.write(('Content-Disposition: form-data; name="%s"; '
                        'filename="%s"\r\n' % (fieldname, filename))\
                       .encode())
            body.write(('Content-Type: %s\r\n\r\n' %
                       (get_content_type(filename))).encode(charset))
        else:
            data = value
            body.write(('Content-Disposition: form-data; name="%s"\r\n'
                        % (fieldname)).encode())
            body.write(b'Content-Type: text/plain\r\n\r\n')

        data = body.write(to_bytes(data))
        body.write(b'\r\n')

    body.write(('--%s--\r\n' % (boundary)).encode(charset))
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return body.getvalue(), content_type

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

def basic_auth_str(username, password):
    """Returns a Basic Auth string."""
    b64 = b64encode(('%s:%s' % (username, password)).encode('latin1'))
    return 'Basic ' + b64.strip().decode('latin1')

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
        for key in 'username', 'realm', 'nonce', 'uri', 'nc', 'cnonce', \
                   'response':
            if not key in auth_map:
                return
        return HTTPDigestAuth(auth_map)

def http_date(epoch_seconds=None):
    """
    Formats the time to match the RFC1123 date format as specified by HTTP
    RFC2616 section 3.3.1.

    Accepts a floating point number expressed in seconds since the epoch, in
    UTC - such as that outputted by time.time(). If set to None, defaults to
    the current time.

    Outputs a string in the format 'Wdy, DD Mon YYYY HH:MM:SS GMT'.
    """
    rfcdate = formatdate(epoch_seconds)
    return '%s GMT' % rfcdate[:25]

#################################################################### COOKIE

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

def cookie_date(epoch_seconds=None):
    """Formats the time to ensure compatibility with Netscape's cookie
    standard.

    Accepts a floating point number expressed in seconds since the epoch in, a
    datetime object or a timetuple.  All times in UTC.  The :func:`parse_date`
    function can be used to parse such a date.

    Outputs a string in the format ``Wdy, DD-Mon-YYYY HH:MM:SS GMT``.

    :param expires: If provided that date is used, otherwise the current.
    """
    rfcdate = formatdate(epoch_seconds)
    return '%s-%s-%s GMT' % (rfcdate[:7], rfcdate[8:11], rfcdate[12:25])

def set_cookie(cookies, key, value='', max_age=None, expires=None, path='/',
               domain=None, secure=False, httponly=False):
    '''Set a cookie key into the cookies dictionary *cookies*.'''
    cookies[key] = value
    if expires is not None:
        if isinstance(expires, datetime):
            delta = expires - expires.utcnow()
            # Add one second so the date matches exactly (a fraction of
            # time gets lost between converting to a timedelta and
            # then the date string).
            delta = delta + timedelta(seconds=1)
            # Just set max_age - the max_age logic will set expires.
            expires = None
            max_age = max(0, delta.days * 86400 + delta.seconds)
        else:
            cookies[key]['expires'] = expires
    if max_age is not None:
        cookies[key]['max-age'] = max_age
        # IE requires expires, so set it if hasn't been already.
        if not expires:
            cookies[key]['expires'] = cookie_date(time.time() + max_age)
    if path is not None:
        cookies[key]['path'] = path
    if domain is not None:
        cookies[key]['domain'] = domain
    if secure:
        cookies[key]['secure'] = True
    if httponly:
        cookies[key]['httponly'] = True

class _ExtendedMorsel(Morsel):
    _reserved = {'httponly': 'HttpOnly'}
    _reserved.update(Morsel._reserved)

    def __init__(self, name=None, value=None):
        Morsel.__init__(self)
        if name is not None:
            self.set(name, value, value)

    def OutputString(self, attrs=None):
        httponly = self.pop('httponly', False)
        result = Morsel.OutputString(self, attrs).rstrip('\t ;')
        if httponly:
            result += '; HttpOnly'
        return result


def dump_cookie(key, value='', max_age=None, expires=None, path='/',
                domain=None, secure=None, httponly=False, charset='utf-8',
                sync_expires=True):
    """Creates a new Set-Cookie header without the ``Set-Cookie`` prefix
    The parameters are the same as in the cookie Morsel object in the
    Python standard library but it accepts unicode data, too.

    :param max_age: should be a number of seconds, or `None` (default) if
                    the cookie should last only as long as the client's
                    browser session.  Additionally `timedelta` objects
                    are accepted, too.
    :param expires: should be a `datetime` object or unix timestamp.
    :param path: limits the cookie to a given path, per default it will
                 span the whole domain.
    :param domain: Use this if you want to set a cross-domain cookie. For
                   example, ``domain=".example.com"`` will set a cookie
                   that is readable by the domain ``www.example.com``,
                   ``foo.example.com`` etc. Otherwise, a cookie will only
                   be readable by the domain that set it.
    :param secure: The cookie will only be available via HTTPS
    :param httponly: disallow JavaScript to access the cookie.  This is an
                     extension to the cookie standard and probably not
                     supported by all browsers.
    :param charset: the encoding for unicode values.
    :param sync_expires: automatically set expires if max_age is defined
                         but expires not.
    """
    morsel = _ExtendedMorsel(key, value)
    if isinstance(max_age, timedelta):
        max_age = (max_age.days * 60 * 60 * 24) + max_age.seconds
    if expires is not None:
        if not isinstance(expires, basestring):
            expires = cookie_date(expires)
        morsel['expires'] = expires
    elif max_age is not None and sync_expires:
        morsel['expires'] = cookie_date(time() + max_age)
    if domain and ':' in domain:
        # The port part of the domain should NOT be used. Strip it
        domain = domain.split(':', 1)[0]
    if domain:
        assert '.' in domain, (
            "Setting \"domain\" for a cookie on a server running localy (ex: "
            "localhost) is not supportted by complying browsers. You should "
            "have something like: \"127.0.0.1 localhost dev.localhost\" on "
            "your hosts file and then point your server to run on "
            "\"dev.localhost\" and also set \"domain\" for \"dev.localhost\""
        )
    for k, v in (('path', path), ('domain', domain), ('secure', secure),
                 ('max-age', max_age), ('httponly', httponly)):
        if v is not None and v is not False:
            morsel[k] = str(v)
    return morsel.output(header='').lstrip()


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

