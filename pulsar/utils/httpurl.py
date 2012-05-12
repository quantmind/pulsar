'''Stand alone HTTP utilities and client library.
This is a thin layer on top of urllib2 in python2 / urllib in Python 3.
'''
import sys
import string
import time
from datetime import datetime, timedelta
from email.utils import formatdate

ispy3k = sys.version_info >= (3,0)

if ispy3k: # Python 3
    from urllib import request as urllibr
    from urllib.parse import quote, unquote, urlencode, urlparse, urlsplit,\
                                parse_qs
    from http.client import responses
    from http.cookiejar import CookieJar
    from http.cookies import SimpleCookie
    
    getproxies_environment = urllibr.getproxies_environment
    ascii_letters = string.ascii_letters
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
    from urllib import quote, unquote, urlencode, getproxies_environment
    from urlparse import urlparse, urlsplit, parse_qs
    from httplib import responses
    from cookielib import CookieJar
    from Cookie import SimpleCookie
    
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
            return unicode(value).encode(encoding, errors)
    
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
    
    
escape = lambda s: quote(s, safe='~')

# The reserved URI characters (RFC 3986 - section 2.2)
URI_GEN_DELIMS = frozenset(':/?#[]@')
URI_SUB_DELIMS = frozenset("!$&'()*+,;=")
URI_RESERVED_SET = URI_GEN_DELIMS.union(URI_SUB_DELIMS)
URI_RESERVED_CHARS = ''.join(URI_RESERVED_SET)
# The unreserved URI characters (RFC 3986 - section 2.3)
URI_UNRESERVED_SET = frozenset(ascii_letters + string.digits + '-._~')

urlquote = lambda iri: quote(iri, safe=URI_RESERVED_CHARS)

def unquote_unreserved(uri):
    """Un-escape any percent-escape sequences in a URI that are unreserved
characters. This leaves all reserved, illegal and non-ASCII bytes encoded."""
    unreserved_set = UNRESERVED_SET
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
    return urlquote(unquote_unreserved(uri))


####################################################    HEADERS
HEADER_FIELDS = {'general': frozenset(('Cache-Control', 'Connection', 'Date',
                                       'Pragma', 'Trailer','Transfer-Encoding',
                                       'Upgrade', 'Via','Warning')),
                 'request': frozenset(('Accept', 'Accept-Charset',
                                       'Accept-Encoding', 'Accept-Language',
                                       'Authorization', 'Expect', 'From',
                                       'Host', 'If-Match', 'If-Modified-Since',
                                       'If-None-Match', 'If-Range',
                                       'If-Unmodified-Since', 'Max-Forwards',
                                       'Proxy-Authorization', 'Range',
                                       'Referer', 'TE', 'User-Agent')),
                 'response': frozenset(('Accept-Ranges', 'Age', 'ETag',
                                        'Location', 'Proxy-Authenticate',
                                        'Retry-After', 'Server', 'Vary',
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
TYPE_HEADER_FIELDS = {'client': CLIENT_HEADER_FIELDS,
                      'server': SERVER_HEADER_FIELDS}
ALL_HEADER_FIELDS = CLIENT_HEADER_FIELDS.union(SERVER_HEADER_FIELDS)

def _capitalbits(col):
    for field in col:
        for bit in field.split('-'):
            if bit.upper() == bit:
                yield bit
                
CAPITAL_HEADER_BITS = frozenset(_capitalbits(ALL_HEADER_FIELDS))

def _header_field(name):
    for bit in name.split('-'):
        bit = bit.upper()
        if bit in CAPITAL_HEADER_BITS:
            yield bit
        else:
            yield bit[0] + bit[1:].lower()
            
def header_field(name):
    return '-'.join(_header_field(name))

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
    def __init__(self, type='server', defaults=None):
        self.type = type.lower()
        self.all_headers = TYPE_HEADER_FIELDS.get(self.type)
        if not self.all_headers:
            raise ValueError('Header must be for client or server. '\
                             'Passed "%s" instead' % type)
        super(Headers, self).__init__()
        if defaults is not None:
            self.update(defaults)
    
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
            if key in self.all_headers:
                if isinstance(value, (tuple, list)):
                    value = ','.join(value)
                super(Headers, self).__setitem__(key, value)
                
    def pop(self, key, *args):
        return super(Headers, self).pop(header_field(key), *args)
    
    def headers(self):
        return list(self.items())
    
    def as_list(self, key):
        '''Return the value at *key* as a list of values.'''
        value = self.get(key)
        value = self._dict.get(key.lower(),None)
        return value.split(',') if value else []
        
    def add(self, key, value):
        '''Add *value* to *key* header. If the header is already available,
append the value to the list.'''
        if value:
            key = header_field(key)
            values = self.as_list(key)
            if value not in values:
                values.append(value)
                self[key] = values
        
    def flat(self, version, status):
        vs = version + (status,)
        h = 'HTTP/{0}.{1} {2}'.format(*vs) 
        f = ''.join(("{0}: {1}\r\n".format(n, v) for n, v in self))
        return '{0}\r\n{1}\r\n'.format(h,f)
    
    @property
    def vary_headers(self):
        return self.get('vary',[])
        
    def has_vary(self, header_query):
        """Checks to see if the has a given header name in its Vary header.
        """
        return header_query.lower() in set(self.vary_headers)
    
    
####################################################    HTTP CLIENT
class HttpClientResponse(object):
    '''Instances of this class are returned from the
:meth:`HttpClientHandler.request` method.

.. attribute:: status_code

    Numeric `status code`_ of the response
    
.. attribute:: url

    Url of request
    
.. attribute:: response

    Status code description
    
.. attribute:: headers

    List of response headers
    
.. attribute:: content

    Body of response
    
.. attribute:: is_error

    Boolean indicating if this is a response error.
    
.. _`status code`: http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
'''
    _resp = None
    status_code = None
    url = None
    HTTPError = urllibr.HTTPError
    URLError = urllibr.URLError 
    
    def __str__(self):
        if self.status_code:
            return '{0} {1}'.format(self.status_code,self.response)
        else:
            return '<None>'
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    @property
    def is_error(self):
        return isinstance(self._resp,Exception)
    
    @property
    def response(self):
        if self.status_code:
            return responses.get(self.status_code)
        
    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`,
 if one occured."""
        if self.is_error:
            raise self._resp
    
    
class Response(HttpClientResponse):
    status_code = None
    
    def __init__(self, response):
        self._resp = response
        self.status_code = getattr(response, 'code', None)
        self.url = getattr(response, 'url', None)
    
    @property
    def headers(self):
        return getattr(self._resp,'headers',None)
    
    @property
    def content(self):
        if not hasattr(self,'_content') and self._resp:
            if hasattr(self._resp,'read'):
                self._content = self._resp.read()
            else:
                self._content = b''
        return getattr(self,'_content',None)
    
    def content_string(self):
        return self.content.decode()
    
    
class HttpClient(object):
    '''Http handler from the standard library'''
    HTTPError = urllibr.HTTPError
    URLError = urllibr.URLError
    DEFAULT_HEADERS = Headers('client',[('Connection', 'Keep-Alive'),
                                        ('User-Agent', 'Python-httpurl')])
    _encode_url_methods = frozenset(['DELETE', 'GET', 'HEAD', 'OPTIONS'])
    _encode_body_methods = frozenset(['PATCH', 'POST', 'PUT', 'TRACE'])
    
    def __init__(self, proxy_info = None,
                 timeout = None, cache = None,
                 headers = None, handle_cookie = False):
        dheaders = dict(self.DEFAULT_HEADERS)
        if headers:
            dheaders.update(headers)
        self.DEFAULT_HEADERS = dheaders
        handlers = []
        if proxy_info:
            handlers.append(urllibr.ProxyHandler(proxy_info))
        if handle_cookie:
            cj = CookieJar()
            handlers.append(urllibr.HTTPCookieProcessor(cj))
        self._opener = urllibr.build_opener(*handlers)
        #self._opener.addheaders = self.get_headers(headers)    
        self.timeout = timeout
        
    def get_headers(self, headers=None):
        if headers:
            d = self.DEFAULT_HEADERS.copy()
            d.extend(headers)
            return d
        else:
            return self.DEFAULT_HEADERS
    
    def get(self, url, body=None):
        '''Sends a GET request and returns a :class:`HttpClientResponse`
object.'''
        return self.request(url, body=body, method='GET')
    
    def post(self, url, body=None):
        return self.request(url, body=body, method='POST')
    
    def request(self, url, body=None, method=None, headers=None,
                timeout=None, **kwargs):
        url, body, method = self._encode(url, body, method)
        try:
            headers = self.get_headers(headers)
            timeout = timeout if timeout is not None else self.timeout
            req = urllibr.Request(url, body, headers)
            response = self._opener.open(req, timeout=timeout)
        except (self.HTTPError, self.URLError) as why:
            return Response(why)
        else:
            return Response(response)
    
    def add_password(self, username, password, uri, realm=None):
        '''Add Basic HTTP Authentication to the opener'''
        if realm is None:
            password_mgr = HTTPPasswordMgrWithDefaultRealm()
        else:
            password_mgr = HTTPPasswordMgr()
        password_mgr.add_password(realm, uri, user, passwd)
        self._opener.add_handler(HTTPBasicAuthHandler(password_mgr))
    
    def _encode(self, url, body, method):
        if not method:
            method = 'POST' if body else 'GET'
        else:
            method = method.upper()    
        if isinstance(body, dict):
            if method in self._encode_body_methods:
                body = '&'.join(('%s=%s' % (escape(k),escape(v))\
                                        for k,v in iteritems(body)))
                body = to_bytes(body)
            else:
                url = iri_to_uri(url, **body)
        elif body:
            if method in self._encode_url_methods:
                url = '%s?%s' % (url, to_string(body))
            else:
                body = to_bytes(body)
        if method in self._encode_url_methods:
            body = None
        return url, body, method
            
        
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