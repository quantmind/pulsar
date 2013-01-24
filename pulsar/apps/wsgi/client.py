import platform

import pulsar
from pulsar.utils.httpurl import *

__all__ = ['HttpClient']

class HttpRequest(object):
    parser_class = HttpParser
    version = 'HTTP/1.1'
    _tunnel_host = None
    _has_proxy = False
    def __init__(self, url, method, data=None, files=None,
                 charset=None, encode_multipart=True, multipart_boundary=None,
                 timeout=None, hooks=None, history=None, source_address=None,
                 allow_redirects=False, max_redirects=10, decompress=True,
                 version=None):
        self.type, self.host, self.path, self.params,\
        self.query, self.fragment = urlparse(url)
        self.full_url = self._get_full_url()
        self.timeout = timeout
        self.hooks = hooks
        self.history = history
        self.max_redirects = max_redirects
        self.allow_redirects = allow_redirects
        self.charset = charset or DEFAULT_CHARSET
        self.method = method.upper()
        self.version = version or self.version
        self.data = data if data is not None else {}
        self.files = files
        self.source_address = source_address
        self._set_hostport(*host_and_port(self.host))
        self.parser = self.parser_class(kind=1, decompress=self.decompress)
        # Pre-request hook.
        self.dispatch_hook('pre_request', self)
        self.encode(encode_multipart, multipart_boundary)
        
    def __hash__(self):
        # For the connection pool
        return (self.type, self.host, self.port, self.timeout)
        
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
                        raise InvalidURL("nonnumeric port: '%s'" % host[i+1:])
                host = host[:i]
            else:
                port = self.default_port
            if host and host[0] == '[' and host[-1] == ']':
                host = host[1:-1]
        self.host = host
        self.port = port
    
    def encode(self):
        self._buffer = buffer = []
        request = '%s %s %s' % (self.method, self.url, self.version)
        buffer.append(request.encode('ascii'))
        buffer.append(bytes(self.headers))
         
        

class HttpResponse(pulsar.ClientProtocolConsumer):
    '''Http client request initialised by a call to the
:class:`HttpClient.request` method.

.. attribute:: client

    The :class:`HttpClient` performing the request

.. attribute:: type

    The scheme of the of the URI requested. One of http, https
'''

    _tunnel_host = None
    _has_proxy = False       
    
    def feed(self, data):
        pass


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
    DEFAULT_HTTP_HEADERS = Headers([
            ('Connection', 'Keep-Alive'),
            ('Accept-Encoding', 'identity'),
            ('Accept-Encoding', 'deflate'),
            ('Accept-Encoding', 'compress'),
            ('Accept-Encoding', 'gzip')],
            kind='client')
    request_parameters = pulsar.Client.request_parameters +\
                        ('encode_multipart', 'max_redirects', 'decompress',
                         'allow_redirects', 'multipart_boundary', 'version')
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
        self.poolmap = {}
        self.cookies = cookies
        self.decompress = decompress
        dheaders = self.DEFAULT_HTTP_HEADERS.copy()
        self.client_version = client_version or self.client_version
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
    
    def request(self, method, url, cookies=None, headers=None, **params):
        '''Constructs and sends a request to a remote server.
It returns an :class:`HttpResponse` object.

:param method: request method for the :class:`HttpRequest`.
:param url: URL for the :class:`HttpRequest`.
:param params: a dictionary which specify all the optional parameters for
the :class:`HttpRequest` constructor.

:rtype: a :class:`HttpResponse` object.
'''
        for parameter in self.request_parameters:
            self._update_parameter(parameter, params)
        request = HttpRequest(url, method, **params)
        self.set_headers(request, headers)
        # Set proxy if required
        self.set_proxy(request)
        if self.cookies:
            self.cookies.add_cookie_header(request)
        if cookies:
            if not isinstance(cookies, CookieJar):
                cookies = cookiejar_from_dict(cookies)
            cookies.add_cookie_header(request)
        return request

    def set_headers(self, request, headers):
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