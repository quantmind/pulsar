'''Stand alone HTTP, URLS utilities and HTTP client library.
This is a thin layer on top of urllib2 in python2 / urllib in Python 3.
To create this module I've used several bits from around the opensorce community
In particular:

* urllib3_
* request_

'''
import sys
import string
import time
import json
import mimetypes
import codecs
import socket
from uuid import uuid4
from datetime import datetime, timedelta
from email.utils import formatdate

ispy3k = sys.version_info >= (3,0)

if ispy3k: # Python 3
    from urllib import request as urllibr
    from http import client as httpclient
    from urllib.parse import quote, unquote, urlencode, urlparse, urlsplit,\
                                parse_qs, splitport, urlunparse
    from http.client import responses
    from http.cookiejar import CookieJar
    from http.cookies import SimpleCookie
    from io import BytesIO, StringIO
    
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
            return '%s' % s
        
    def native_str(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        else:
            return s
        
    def force_native_str(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        else:
            return '%s' % s
        
else:   # pragma : no cover
    import urllib2 as urllibr
    import httplib as httpclient
    from urllib import quote, unquote, urlencode, getproxies_environment, \
                        splitport
    from urlparse import urlparse, urlsplit, parse_qs, urlunparse
    from httplib import responses
    from cookielib import CookieJar
    from Cookie import SimpleCookie
    from cStringIO import StringIO
    from itertools import izip as zip, imap as map
    
    BytesIO = StringIO
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
        
    def native_str(s):
        if isinstance(s, unicode):
            return s.encode('utf-8')
        else:
            return s
        
    def force_native_str(s):
        if isinstance(s, unicode):
            return s.encode('utf-8')
        else:
            return '%s' % s
    
HTTPError = urllibr.HTTPError
URLError = urllibr.URLError
    
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
    parts = uri.split('%')
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

####################################################    REQUEST METHODS
ENCODE_URL_METHODS = frozenset(['DELETE', 'GET', 'HEAD', 'OPTIONS'])
ENCODE_BODY_METHODS = frozenset(['PATCH', 'POST', 'PUT', 'TRACE'])

def has_empty_content(status, method=None):
    '''204, 304 and 1xx codes have no content'''
    if status == httpclient.NO_CONTENT or\
            status == httpclient.NOT_MODIFIED or\
            100 <= status < 200 or\
            method == "HEAD":
        return True
    else:
        return False
    
####################################################    HEADERS
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
                                       'Authorization', 'Expect', 'From',
                                       'Host', 'If-Match', 'If-Modified-Since',
                                       'If-None-Match', 'If-Range',
                                       'If-Unmodified-Since', 'Max-Forwards',
                                       'Proxy-Authorization', 'Range',
                                       'Referer',
                                       'Sec-WebSocket-Key',
                                       'Sec-WebSocket-Version',
                                       'TE', 'User-Agent')),
                 # The response-header fields allow the server to pass
                 # additional information about the response which cannot be
                 # placed in the Status- Line.
                 'response': frozenset(('Accept-Ranges', 'Age', 'ETag',
                                        'Location', 'Proxy-Authenticate',
                                        'Retry-After',
                                        'Sec-WebSocket-Accept',
                                        'Server', 'Vary',
                                        'WWW-Authenticate')),
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
            
def header_field(name):
    return ALL_HEADER_FIELDS_DICT.get(name.lower())


class Headers(dict):
    '''Utility for managing HTTP headers for both clients and servers.
It has a dictionary like interface
with few extra functions to facilitate the insertion of multiple values.

From http://www.w3.org/Protocols/rfc2616/rfc2616.html

Section 4.2
The order in which header fields with differing field names are received is not
significant. However, it is "good practice" to send general-header fields first,
followed by request-header or response-header fields, and ending with
the entity-header fields.'''    
    def __init__(self, data=None, kind='server'):
        if isinstance(kind, int):
            kind = header_type.get(kind, 'both')
        else:
            kind = kind.lower()
        self.kind = kind
        self.all_headers = TYPE_HEADER_FIELDS.get(self.kind)
        if not self.all_headers:
            self.kind = 'both'
            self.all_headers = TYPE_HEADER_FIELDS[self.kind]
        super(Headers, self).__init__()
        if data:
            self.update(data)
    
    def __repr__(self):
        return '%s %s' % (self.kind, super(Headers,self).__repr__())
    
    @property
    def kind_number(self):
        return header_type_to_int.get(self.kind)
    
    def update(self, iterable):
        """Extend the headers with a dictionary or an iterable yielding keys and
        values.
        """
        if isinstance(iterable, dict):
            iterable = iteritems(iterable)
        set = self.__setitem__
        for key, value in iterable:
            set(key, value)
        
    def __iter__(self):
        hf = HEADER_FIELDS
        order = (('general',[]), ('request',[]), ('response',[]), ('entity',[]))
        for key in super(Headers, self).__iter__():
            for name, group in order:
                if key in hf[name]:
                    group.append(key)
                    break
        for _, group in order:
            for k in group:
                yield k

    def __contains__(self, key):
        return super(Headers, self).__contains__(header_field(key))
    
    def __getitem__(self, key):
        return super(Headers, self).__getitem__(header_field(key))
    
    def __delitem__(self, key):
        return super(Headers, self).__delitem__(header_field(key))

    def __setitem__(self, key, value):
        if value:
            key = header_field(key)
            if key and key in self.all_headers:
                if isinstance(value, (tuple, list)):
                    value = ', '.join(value)
                super(Headers, self).__setitem__(key, value)
    
    def get(self, key, default=None):
         return super(Headers, self).get(header_field(key),default)
    
    def pop(self, key, *args):
        return super(Headers, self).pop(header_field(key), *args)
    
    def copy(self):
        return self.__class__(self, kind=self.kind)
        
    def headers(self):
        return list(self.items())
    
    def as_list(self, key, default=None):
        '''Return the value at *key* as a list of values.'''
        value = self.get(key)
        return value.split(', ') if value else default
    get_all = as_list
    getheaders = as_list
    
    def add(self, key, value):
        '''Add *value* to *key* header. If the header is already available,
append the value to the list.'''
        if value:
            key = header_field(key)
            values = self.as_list(key, [])
            if value not in values:
                values.append(value)
                self[key] = values
        
    def flat(self, version, status):
        vs = version + (status,)
        h = 'HTTP/%s.%s %s' % vs 
        f = ''.join(("%s: %s\r\n" % kv for kv in iteritems(self)))
        return '%s\r\n%s\r\n' % (h, f)
         
    @property
    def vary_headers(self):
        return self.get('vary',[])
        
    def has_vary(self, header_query):
        """Checks to see if the has a given header name in its Vary header.
        """
        return header_query.lower() in set(self.vary_headers)
    
    
####################################################    HTTP CLIENT    
class HttpConnectionError(Exception):
    pass


class HTTPResponseMixin(object):
        
    @property
    def status_code(self):
        return getattr(self, 'code', None)

    def __str__(self):
        if self.status_code:
            return '{0} {1}'.format(self.status_code,self.response)
        else:
            return '<None>'
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    def content_string(self, charset=None):
        return self.content.decode(charset or 'utf-8')
    
    def content_json(self, charset=None, **kwargs):
        '''Decode content using json.'''
        return json.loads(self.content_string(charset), **kwargs)
    
    @property
    def content(self):
        if not hasattr(self,'_content'):
            self._content = self.read()
        return getattr(self, '_content', None)
    
    @property
    def is_error(self):
        if self.status_code:
            return not (200 <= self.status_code < 300)
    
    @property
    def response(self):
        if self.status_code:
            return responses.get(self.status_code)
    
    def add_callback(self, callback):
        self.callbacks.append(callback)
        return self
        
    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`,
 if one occured."""
        if self.is_error:
            raise HTTPError(self.url, self.status_code,
                            self.content, self.headers, None)        
    
    def post_process_response(self, client, req):
        return client.post_process_response(req, self)
    
    
class HTTPResponse(HTTPResponseMixin, httpclient.HTTPResponse):
    pass
    
    
class HTTPConnection(httpclient.HTTPConnection):
    response_class = HTTPResponse
    
    @property
    def is_async(self):
        return self.timeout == 0


class Request(object):
    '''
* Default is charset is "iso-8859-1" (latin-1) from section 3.7.1

http://www.ietf.org/rfc/rfc2616.txt 
    '''
    default_charset = 'latin-1'
    DEFAULT_PORTS = {'http': 80, 'https': 443}
    
    def __init__(self, url, method=None, data=None, charset=None,
                 encode_multipart=True, multipart_boundary=None,
                 headers=None, timeout=None, **kwargs):
        self.type, self.host, self.path, self.params,\
        self.query, self.fragment = urlparse(url)
        self.full_url = self._get_full_url()
        self.data = None
        self._has_proxy = False
        self.timeout = timeout
        self.headers = {}
        self._tunnel_host = None
        self.connection = None
        if headers:
            for key, value in iteritems(headers):
                self.add_header(key, value)
        self.charset = charset or self.default_charset
        self._encode(method, data, encode_multipart, multipart_boundary)
    
    @property
    def selector(self):
        if self.has_proxy():
            return self.full_url
        elif self.query:
            return '%s?%s' % (self.path,self.query)
        else:
            return self.path        
        
    def get_response(self, headers):
        h = self.connection
        h.request(self.get_method(), self.selector, self.data, headers)
        r = h.getresponse()
        r.protocol = self.type
        r.url = self.full_url
        return r
    
    def get_type(self):
        return self.type
    
    def get_method(self):
        return self.method
    
    def host_and_port(self):
        return host_and_port(self.host)
    
    def add_header(self, key, value):
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
        return (self.type, host, port)            
            
    def _encode(self, method, body, encode_multipart, multipart_boundary):
        if not method:
            method = 'POST' if body else 'GET'
        else:
            method = method.upper()
        self.method = method
        if method in ENCODE_URL_METHODS:
            self._encode_url(body)
        else:
            content_type = 'application/x-www-form-urlencoded'
            if isinstance(body, (dict, list, tuple)):
                if encode_multipart:
                    body, content_type = encode_multipart_formdata(body,
                                                    boundary=multipart_boundary,
                                                    charset=self.charset)
                else:
                    body = urlencode(body)
            elif body:
                body = to_bytes(body, self.charset)
            self.data = body
            self.headers['Content-Type'] = content_type
        
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
            

class HttpConnectionPool(object):
    
    def __init__(self, Connections, max_connections, timeout, scheme,
                 host, port=None):
        self.Connections = Connections
        self.scheme = scheme
        self.host = host
        self.port = port
        self.timeout = timeout
        self.max_connections = max_connections or 2**31
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        
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
        if self._created_connections >= self.max_connections:
            raise HttpConnectionError("Too many connections")
        self._created_connections += 1
        connection_class = self.Connections.get(self.scheme)
        if not connection_class:
            raise HttpConnectionError('Could not create connection')
        c = connection_class(self.host, self.port)
        if self.timeout is not None:
            c.timeout = self.timeout
        return c

    def release(self, connection):
        "Releases the connection back to the pool"
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)
    
    def remove(self, connection):
        self._in_use_connections.remove(connection)
        

class HttpHandler(urllibr.AbstractHTTPHandler):
    '''A modified Http handler'''
    def __init__(self, client, debuglevel=0):
        if ispy3k:
            super(HttpHandler, self).__init__(debuglevel)
        else:
            urllibr.AbstractHTTPHandler.__init__(self, debuglevel)
        self.client = client
        
    def http_open(self, req):
        connection = self.client.get_connection(req)
        return self.do_open(connection, req)
    
    def do_open(self, h, req):
        req.connection = h
        headers = req.headers
        if req._tunnel_host:
            tunnel_headers = {}
            proxy_auth_hdr = "Proxy-Authorization"
            if proxy_auth_hdr in headers:
                tunnel_headers[proxy_auth_hdr] = headers[proxy_auth_hdr]
                # Proxy-Authorization should not be sent to origin
                # server.
                del headers[proxy_auth_hdr]
            h.set_tunnel(req._tunnel_host, headers=tunnel_headers)
        try:
            return req.get_response(headers)
        except socket.error as err:
            # This is an asynchronous socket
            if err.errno != 10035:
                self.client.close_connection(req)
                raise URLError(err)
        except:
            self.client.close_connection(req)
            raise
        
    
class HttpClient(urllibr.OpenerDirector):
    '''A client of a networked server'''
    request_class = Request
    client_version = 'Python-httpurl'
    Connections = {'http': HTTPConnection}
    DEFAULT_HTTP_HEADERS = Headers([
            ('Connection', 'Keep-Alive'),
            ('Accept-Encoding', ('identity', 'deflate', 'compress', 'gzip'))],
            kind='client')
    
    def __init__(self, proxy_info = None, timeout=None, cache = None,
                 headers=None, encode_multipart=True, client_version=None,
                 multipart_boundary=None, max_connections=None):
        if ispy3k:
            super(HttpClient, self).__init__()
        else:
            urllibr.OpenerDirector.__init__(self)
        self.poolmap = {}
        self.timeout = timeout
        self.max_connections = max_connections
        dheaders = self.DEFAULT_HTTP_HEADERS.copy()
        self.client_version = client_version or self.client_version
        dheaders['user-agent'] = self.client_version
        if headers:
            dheaders.update(headers)
        self.DEFAULT_HTTP_HEADERS = dheaders
        self.add_handler(urllibr.ProxyHandler(proxy_info))
        self.add_handler(urllibr.HTTPCookieProcessor(CookieJar()))
        self.add_handler(HttpHandler(self))
        self.encode_multipart = encode_multipart
        self.multipart_boundary = multipart_boundary or choose_boundary()
        
    def get_headers(self, headers=None):
        d = self.DEFAULT_HTTP_HEADERS.copy()
        if headers:
            d.extend(headers)
        return d
    
    def get(self, url, body=None, headers=None):
        '''Sends a GET request and returns a :class:`HttpClientResponse`
object.'''
        return self.request(url, body=body, method='GET', headers=headers)
    
    def post(self, url, body=None, headers=None):
        return self.request(url, body=body, method='POST', headers=headers)
    
    def request(self, url, body=None, method=None, headers=None,
                timeout=None, encode_multipart=None, allow_redirects=True,
                **kwargs):
        headers = self.get_headers(headers)
        encode_multipart = encode_multipart if encode_multipart is not None\
                            else self.encode_multipart
        request = self.request_class(url, method=method, data=body,
                                     headers=headers, timeout=timeout,
                                     encode_multipart=encode_multipart,
                                     multipart_boundary=self.multipart_boundary)
        # pre-process request
        protocol = request.type
        meth_name = protocol+"_request"
        for processor in self.process_request.get(protocol, ()):
            meth = getattr(processor, meth_name)
            request = meth(request)
            
        response = self._open(request, request.data)
        return response.post_process_response(self, request)
    
    def get_connection(self, request):
        key = request.key
        pool = self.poolmap.get(key)
        if pool is None:
            pool = HttpConnectionPool(self.Connections,
                                      self.max_connections,
                                      self.timeout, *key)
            self.poolmap[key] = pool
        return pool.get_connection()
    
    def close_connection(self, req):
        key = req.key
        pool = self.poolmap.get(key)
        if pool:
            pool.remove(req.connection)
        
    def post_process_response(self, req, response):
        protocol = req.type
        meth_name = protocol+"_response"
        for processor in self.process_response.get(protocol, []):
            meth = getattr(processor, meth_name)
            response = meth(req, response)
        return response
        
    def add_password(self, username, password, uri, realm=None):
        '''Add Basic HTTP Authentication to the opener'''
        if realm is None:
            password_mgr = HTTPPasswordMgrWithDefaultRealm()
        else:
            password_mgr = HTTPPasswordMgr()
        password_mgr.add_password(realm, uri, user, passwd)
        self._opener.add_handler(HTTPBasicAuthHandler(password_mgr))
            

####################################################    ENCODERS AND PARSERS
def choose_boundary():
    """
    Our embarassingly-simple replacement for mimetools.choose_boundary.
    """
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


class DictPropertyMixin(object):
    properties = ()
    def __init__(self, data = None, properties = None):
        self.data = data or {}
        self.properties = properties or self.properties
        
    def __getattr__(self, name):
        if name not in self.data:
            if name not in self.properties:
                raise AttributeError
            else:
                return None
        return self.data[name]
    
    
class Authorization(DictPropertyMixin):
    """Represents an `Authorization` header sent by the client."""

    def __init__(self, auth_type, data=None):
        super(Authorization,self).__init__(data = data)
        self.type = auth_type
        
        
def parse_authorization_header(value):
    """Parse an HTTP basic/digest authorization header transmitted by the web
browser.  The return value is either `None` if the header was invalid or
not given, otherwise an :class:`Authorization` object.

:param value: the authorization header to parse.
:return: a :class:`Authorization` object or `None`."""
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
    '''Set cookies'''
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