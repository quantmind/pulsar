import platform
import json

import pulsar
from pulsar import create_transport
from pulsar.utils.pep import native_str, is_string
from pulsar.utils.structures import mapping_iterator
from pulsar.utils.websocket import FrameParser
from pulsar.utils.httpurl import urlparse, urljoin, DEFAULT_CHARSET,\
                                    REDIRECT_CODES, HttpParser, httpclient,\
                                    ENCODE_URL_METHODS, parse_qsl,\
                                    encode_multipart_formdata, urlencode,\
                                    Headers, urllibr, get_environ_proxies,\
                                    choose_boundary, urlunparse,\
                                    host_and_port, responses, is_succesful,\
                                    HTTPError, request_host

from .plugins import *

__all__ = ['HttpClient']


class TooManyRedirects(Exception):
    pass


class HttpRequest(pulsar.Request):
    parser_class = HttpParser
    _tunnel_host = None
    _has_proxy = False
    def __init__(self, client, url, method, data=None, files=None,
                  charset=None, encode_multipart=True, multipart_boundary=None,
                  timeout=None, hooks=None, history=None, source_address=None,
                  allow_redirects=False, max_redirects=10, decompress=True,
                  version=None, wait_continue=False, **ignored):
        self.client = client
        self.type, self.full_host, self.path, self.params,\
        self.query, self.fragment = urlparse(url)
        self.full_url = self._get_full_url()
        self._set_hostport(*host_and_port(self.full_host))
        super(HttpRequest, self).__init__((self.host, self.port), timeout)
        #self.bind_event(hooks)
        self.history = history
        self.wait_continue = wait_continue
        self.max_redirects = max_redirects
        self.allow_redirects = allow_redirects
        self.charset = charset or 'utf-8'
        self.method = method.upper()
        self.version = version
        self.decompress = decompress
        self.encode_multipart = encode_multipart 
        self.multipart_boundary = multipart_boundary
        self.data = data if data is not None else {}
        self.files = files
        self.source_address = source_address
        self.parser = self.parser_class(kind=1, decompress=self.decompress)
        
    def __hash__(self):
        return hash((self.type, self.address, self.timeout))
    
    def __repr__(self):
        return self.first_line()
    
    def __str__(self):
        return self.__repr__()
    
    def first_line(self):
        return '%s %s %s' % (self.method, self.full_url, self.version)
    
    def set_proxy(self, host, type):
        if self.type == 'https' and not self._tunnel_host:
            self._tunnel_host = self.host
        else:
            self.type = type
            self._has_proxy = True
        self.host = host
        
    @property
    def default_port(self):
        return httpclient.HTTPS_PORT if self.type == 'https' else\
                 httpclient.HTTP_PORT
                  
    def _set_hostport(self, host, port):
        if port is None:
            i = host.rfind(':')
            j = host.rfind(']')         # ipv6 addresses have [...]
            if i > j:
                try:
                    port = int(host[i+1:])
                except ValueError:
                    if host[i+1:] == "": # http://foo.com:/ == http://foo.com/
                        port = self.default_port
                    else:
                        raise httpclient.InvalidURL("nonnumeric port: '%s'"
                                                     % host[i+1:])
                host = host[:i]
            else:
                port = self.default_port
            if host and host[0] == '[' and host[-1] == ']':
                host = host[1:-1]
        self.host = host
        self.port = port
    
    def encode(self):
        buffer = []
        self.body = body = self.encode_body()
        if body:
            #self.headers['content-length'] = str(len(body))
            if self.wait_continue:
                self.headers['expect'] = '100-continue'
                body = None
        request = self.first_line()
        buffer.append(request.encode('ascii'))
        buffer.append(bytes(self.headers))
        buffer.append(b'')
        buffer.append(b'')
        if body:
            buffer.append(body)
        return b'\r\n'.join(buffer)
        
    def encode_body(self):
        '''Encode body or url if the method does not have body'''
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
                if self.encode_multipart:
                    body, content_type = encode_multipart_formdata(
                                            self.data,
                                            boundary=self.multipart_boundary,
                                            charset=self.charset)
                else:
                    body = urlencode(self.data).encode(self.charset)
                self.headers['Content-Type'] = content_type
            elif content_type == 'application/json':
                body = json.dumps(self.data).encode(self.charset)
            else:
                body = json.dumps(self.data).encode(self.charset)
        return body
         
    def params(self):
        return self.__dict__.copy()
    
    def _encode_url(self, body):
        query = self.query
        if body:
            body = native_str(body)
            if isinstance(body, str):
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
        return urlunparse((self.type, self.full_host, self.path,
                                   self.params, self.query, ''))
        
        
class HttpResponse(pulsar.ProtocolConsumer):
    '''Http client request initialised by a call to the
:class:`HttpClient.request` method.

.. attribute:: client

    The :class:`HttpClient` performing the request

.. attribute:: type

    The scheme of the of the URI requested. One of http, https
'''
    ONE_TIME_EVENTS = ('start', 'headers', 'finish')
    _tunnel_host = None
    _has_proxy = False
    consumer = None # Remove default consumer
    next_url = None
    
    def __init__(self, connection, request, consumer=None):
        super(HttpResponse, self).__init__(connection, request, consumer)
        self._buffer = []
        self._headers = None
    
    @property
    def parser(self):
        return self.request.parser
    
    @property
    def content(self):
        return b''.join(self._buffer)
    
    def __str__(self):
        if self.status_code:
            return '%s %s' % (self.status_code, self.response)
        else:
            return '<None>'

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self)

    @property
    def status_code(self):
        if self.parser:
            return self.parser.get_status_code()
    
    @property
    def response(self):
        if self.status_code:
            return responses.get(self.status_code)
        
    @property
    def url(self):
        return self.request.full_url
    
    @property
    def history(self):
        if self.request is not None:
           return self.request.history
    
    @property
    def headers(self):
        if self._headers is None:
            if self.parser and self.parser.is_headers_complete():
                self._headers = Headers(self.parser.get_headers())
        return self._headers
    
    @property
    def is_error(self):
        if self.status_code:
            return not is_succesful(self.status_code)
        
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
    
    def get_origin_req_host(self):
        response = self.history[-1] if self.history else self
        return request_host(request)
    
    ############################################################################
    ##    PROTOCOL IMPLEMENTATION
    def data_received(self, data):
        had_headers = self.parser.is_headers_complete()
        if self.parser.execute(data, len(data)) == len(data):
            if self.parser.is_headers_complete():
                body = self.parser.recv_body()
                self._buffer.append(body)
                self.fire_event('data_received', body)
                if had_headers:
                    self.fire_event('headers')
                    new_response = self.handle_headers()
                    if new_response:
                        return
                if self.parser.is_message_complete():
                    self.finished()
        else:
            raise pulsar.ProtocolError
        
    def close(self):
        if self.parser.is_message_complete():
            self.finished()
            if self.next_url:
                return self.new_request(self.next_url)
            return self
        
    def handle_headers(self):
        '''The response headers are available. Build the response.'''
        request = self.request
        headers = self.headers
        client = request.client
        # store cookies in clinet if needed
        if client.store_cookies and 'set-cookie' in headers:
            client.cookies.extract_cookies(self, request)
        # check redirect
        if self.status_code in REDIRECT_CODES and 'location' in headers and\
                request.allow_redirects and self.parser.is_message_complete():
            # done with current response
            url = headers.get('location')
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
            return self.new_request(url)
        elif self.status_code == 101:
            # Upgrading response handler
            return client.upgrade(response)
    
                
    def upgrade(self, response):
        self.parser = FrameParser(kind=1)
        client = WebSocketClient(response.sock, response.url)
        client.handshake = response
        return client
    
    def new_request(self, url=None):
        self.finished()
        request = self.request
        params = request.params()
        url = url or request.full_url
        history = params.pop('history') or []
        if len(history) >= request.max_redirects:
            raise TooManyRedirects
        history.append(self)
        method = params.pop('method')
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.3.4
        if response.status_code is 303:
            method = 'GET'
            params.pop('data')
            params.pop('files')
        headers = params.pop('headers')
        headers.pop('Cookie', None)
        params.pop('hooks')
        # Build a new request
        return request.client.request(method, url, headers=headers,
                                      history=history, **params)


class HttpClient(pulsar.Client):
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
    consumer_factory = HttpResponse
    allow_redirects = False
    max_redirects = 10
    client_version = 'Python-httpurl'
    version = 'HTTP/1.1' 
    DEFAULT_HTTP_HEADERS = Headers([
            ('Connection', 'Keep-Alive'),
            ('Accept-Encoding', 'identity'),
            ('Accept-Encoding', 'deflate'),
            ('Accept-Encoding', 'compress'),
            ('Accept-Encoding', 'gzip')],
            kind='client')
    request_parameters = ('encode_multipart', 'max_redirects', 'decompress',
                          'allow_redirects', 'multipart_boundary', 'version',
                          'timeout')
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxy_info dictionary
    no_proxy = set(('localhost', urllibr.localhost(), platform.node()))

    def setup(self, proxy_info=None, cache=None, headers=None,
              encode_multipart=True, multipart_boundary=None,
              key_file=None, cert_file=None, cert_reqs='CERT_NONE',
              ca_certs=None, cookies=None, store_cookies=True,
              max_redirects=10, decompress=True):
        self.store_cookies = store_cookies
        self.max_redirects = max_redirects
        self.cookies = cookies
        self.decompress = decompress
        dheaders = self.DEFAULT_HTTP_HEADERS.copy()
        dheaders['user-agent'] = self.client_version
        if headers:
            dheaders.update(headers)
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
    
    def request(self, method, url, cookies=None, headers=None,
                consumer=None, **params):
        '''Constructs and sends a request to a remote server.
It returns an :class:`HttpResponse` object.

:param method: request method for the :class:`HttpRequest`.
:param url: URL for the :class:`HttpRequest`.
:param params: a dictionary which specify all the optional parameters for
the :class:`HttpRequest` constructor.

:rtype: a :class:`HttpResponse` object.
'''
        params = self.update_parameters(self.request_parameters, params)
        request = HttpRequest(self, url, method, **params)
        request.headers = self.get_headers(request, headers)
        if self.cookies:
            self.cookies.add_cookie_header(request)
        if cookies:
            if not isinstance(cookies, CookieJar):
                cookies = cookiejar_from_dict(cookies)
            cookies.add_cookie_header(request)
        response = self.response(request, consumer)
        response.transport.write(request.encode())
        return response
    
    def add_basic_authentication(self, username, password):
        '''Add a :class:`HTTPBasicAuth` handler to the *pre_requests* hooks.'''
        self.bind_event('pre_request', HTTPBasicAuth(username, password))
        
    def add_digest_authentication(self, username, password):
        self.bind_event('pre_request', HTTPDigestAuth(username, password))

    def get_headers(self, request, headers):
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
    
    def set_proxy(self, request):
        if request.type in self.proxy_info:
            hostonly, _ = splitport(request.host)
            no_proxy = [n for n in self.proxy_info.get('no','').split(',') if n]
            if not any(map(hostonly.endswith, no_proxy)):
                p = urlparse(self.proxy_info[request.type])
                request.set_proxy(p.netloc, p.scheme)
                
    def build_protocol(self, address, timeout):
        type, address = address[0], address[1:] 
        return create_connection(address, timeout)