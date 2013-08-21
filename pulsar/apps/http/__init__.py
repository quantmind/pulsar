'''An :class:`HttpClient` for asynchronous HTTP requests. It can also be used
in synchronous mode by passing ``force_sync`` during initialization::

    >>> from pulsar.apps import http
    >>> client = http.HttpClient()
    >>> response = http.get('http://www.bbc.co.uk')
    
.. _`uri scheme`: http://en.wikipedia.org/wiki/URI_scheme
'''
import os
import platform
import json
from copy import copy
from collections import namedtuple
from base64 import b64encode, b64decode

import pulsar
from pulsar import is_failure
from pulsar.utils.pep import native_str, is_string, to_bytes
from pulsar.utils.structures import mapping_iterator
from pulsar.utils.websocket import FrameParser, SUPPORTED_VERSIONS
from pulsar.apps.wsgi import HTTPBasicAuth, HTTPDigestAuth
from pulsar.utils.httpurl import urlparse, urljoin, DEFAULT_CHARSET,\
                                    REDIRECT_CODES, http_parser, httpclient,\
                                    ENCODE_URL_METHODS, parse_qsl,\
                                    encode_multipart_formdata, urlencode,\
                                    Headers, urllibr, get_environ_proxies,\
                                    choose_boundary, urlunparse,\
                                    host_and_port, responses, is_succesful,\
                                    HTTPError, URLError, request_host,\
                                    requote_uri, get_hostport

from .plugins import *


scheme_host = namedtuple('scheme_host', 'scheme netloc')

class TooManyRedirects(Exception):
    pass


class HttpRequest(pulsar.Request):
    '''An :class:`HttpClient` request for an HTTP resource.
    
.. attribute:: method

    The request method
    
.. attribute:: version

    HTTP version for this request, usually ``HTTP/1.1``

.. attribute:: wait_continue

    if ``True``, the :class:`HttpRequest` includes the
    ``Expect: 100-Continue`` header.
    
'''
    full_url = None
    _proxy = None
    def __init__(self, client, url, method, data=None, files=None,
                  charset=None, encode_multipart=True, multipart_boundary=None,
                  timeout=None, hooks=None, history=None, source_address=None,
                  allow_redirects=False, max_redirects=10, decompress=True,
                  version=None, wait_continue=False, websocket_handler=None,
                  **ignored):
        self.client = client
        self.timeout = timeout
        self.method = method.upper()
        self._scheme, self._netloc, self.path, self.params,\
        self.query, self.fragment = urlparse(url)
        if not self._netloc:
            if self.method == 'CONNECT':
                self._netloc = self.path
                self.path = ''
        self.host_only, self.port = get_hostport(self._scheme, self._netloc)
        self.history = history or []
        self.wait_continue = wait_continue
        self.max_redirects = max_redirects
        self.allow_redirects = allow_redirects
        self.charset = charset or 'utf-8'
        self.version = version
        self.decompress = decompress
        self.encode_multipart = encode_multipart 
        self.multipart_boundary = multipart_boundary
        self.websocket_handler = websocket_handler
        self.data = data if data is not None else {}
        self.files = files
        self.source_address = source_address
        self.new_parser()
    
    @property
    def address(self):
        '''``(host, port)`` tuple of the HTTP resource'''
        return (self.host_only, int(self.port))
    
    @property
    def key(self):
        return (self.scheme, self.address, self.timeout)
    
    @property
    def scheme(self):
        '''The `uri scheme`_ of the HTTP resource.'''
        if self._proxy:
            return max(self._scheme, self._proxy.scheme)
        else:
            return self._scheme
        
    @property
    def netloc(self):
        if self._proxy:
            return self._proxy.netloc
        else:
            return self._netloc
        
    def __repr__(self):
        return self.first_line()
    
    def __str__(self):
        return self.__repr__()
    
    def first_line(self):
        url = self._get_url()
        return '%s %s %s' % (self.method, url, self.version)
    
    def new_parser(self):
        self.parser = http_parser(kind=1, decompress=self.decompress)
        
    def set_proxy(self, scheme, host):
        self.host_only, self.port = get_hostport(scheme, host)
        self._proxy = scheme_host(scheme, host)
    
    def encode(self):
        '''Called by :class:`HttpResponse` when it needs to encode this
:class:`HttpRequest` before sending it to the HTTP resourse.
It returns ``bytes``.'''
        buffer = []
        self.body = body = self.encode_body()
        if body:
            self.headers['content-length'] = str(len(body))
            if self.wait_continue:
                self.headers['expect'] = '100-continue'
                body = None
        request = self.first_line()
        buffer.append(request.encode('ascii'))
        buffer.append(b'\r\n')
        buffer.append(bytes(self.headers))
        if body:
            buffer.append(body)
        return b''.join(buffer)
        
    def encode_body(self):
        '''Encode body or url if the :attr:`method` does not have body.
Called by :meth:`encode`.'''
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
         
    def all_params(self):
        d = self.__dict__.copy()
        d.pop('client')
        d.pop('method')
        return d
    
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

    def _get_url(self):
        self.full_url = urlunparse((self._scheme, self._netloc, self.path,
                                    self.params, self.query, self.fragment))
        if not self._proxy:
            return urlunparse(('', '', self.path or '/', self.params,
                               self.query, self.fragment))
        else:
            return self.full_url
        
        
class HttpResponse(pulsar.ProtocolConsumer):
    '''A :class:`pulsar.ProtocolConsumer` for the HTTP client protocol.
Initialised by a call to the :class:`HttpClient.request` method.
'''
    _tunnel_host = None
    _has_proxy = False
    _content = None
    
    @property
    def parser(self):
        if self.current_request:
            return self.current_request.parser
    
    def recv_body(self):
        '''Flush the response body and return it.'''
        return self.parser.recv_body()
    
    def get_content(self):
        '''Retrieve the body without flushing'''
        b = self.parser.recv_body()
        if b or self._content is None:
            self._content = self._content + b if self._content else b
        return self._content
    
    def __str__(self):
        return self.status or '<None>'

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
    def status(self):
        status_code = self.status_code
        if status_code:
            return '%s %s' % (status_code, responses.get(status_code))
        
    @property
    def url(self):
        if self.current_request is not None:
            return self.current_request.full_url
    
    @property
    def history(self):
        if self.current_request is not None:
           return self.current_request.history
    
    @property
    def headers(self):
        if not hasattr(self, '_headers'):
            if self.parser and self.parser.is_headers_complete():
                self._headers = Headers(self.parser.get_headers())
        return getattr(self, '_headers', None)
    
    @property
    def is_error(self):
        if self.status_code:
            return not is_succesful(self.status_code)
        elif self.on_finished.done():
            return is_failure(self.on_finished.result)
        else:
            return False
    
    def content_string(self, charset=None, errors=None):
        '''Decode content as a string.'''
        data = self.get_content()
        if data is not None:
            return data.decode(charset or 'utf-8', errors or 'strict')

    def content_json(self, charset=None, **kwargs):
        '''Decode content as a JSON object.'''
        return json.loads(self.content_string(charset), **kwargs)
    
    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`,
 if one occured."""
        if self.is_error:
            if self.status_code:
                raise HTTPError(self.url, self.status_code,
                                self.content_string(), self.headers, None)
            else:
                raise URLError(self.on_finished.result.error)
    
    def get_origin_req_host(self):
        response = self.history[-1] if self.history else self
        return request_host(request)
    
    ############################################################################
    ##    PROTOCOL IMPLEMENTATION
    def start_request(self):
        self.transport.write(self.current_request.encode())
        
    def data_received(self, data):
        had_headers = self.parser.is_headers_complete()
        if self.parser.execute(data, len(data)) == len(data):
            if self.parser.is_headers_complete():
                if not had_headers:
                    new_response = self.handle_headers()
                    if new_response:
                        had_headers = False
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
        request = self.current_request
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
            if len(self.history) >= request.max_redirects:
                raise TooManyRedirects
            url = url or self.url
            return client.request(request.method, url, response=self)
        elif self.status_code == 100:
            request.new_parser()
            self.transport.write(request.body)
            return True
        elif self.status_code == 101:
            # Upgrading response handler
            return client.upgrade(self)
    

class HttpClient(pulsar.Client):
    '''A :class:`pulsar.Client` for HTTP/HTTPS servers which handles
a pool of asynchronous :class:`pulsar.Connection`.

.. attribute:: headers

    Default headers for this :class:`HttpClient`.

    Default: :attr:`DEFAULT_HTTP_HEADERS`.

.. attribute:: cookies

    Default cookies for this :class:`HttpClient`

    Default: ``None``.

.. attribute:: timeout

    Default timeout for the connecting sockets. If 0 it is an asynchronous
    client.

.. attribute:: encode_multipart

    Flag indicating if body data is encoded using the ``multipart/form-data``
    encoding by default. It can be overwritten during a :meth:`request`.

    Default: ``True``

.. attribute:: proxy_info

    Dictionary of proxy servers for this client.
    
.. attribute:: DEFAULT_HTTP_HEADERS

    Default headers for this :class:`HttpClient`
    
'''
    consumer_factory = HttpResponse
    allow_redirects = False
    max_redirects = 10
    client_version = 'Python-httpurl'
    version = 'HTTP/1.1' 
    '''Default HTTP request version for this :class:`HttpClient`. It can be
    overwritten during a :meth:`request`.'''
    DEFAULT_HTTP_HEADERS = Headers([
            ('Connection', 'Keep-Alive'),
            ('Accept-Encoding', 'identity'),
            ('Accept-Encoding', 'deflate'),
            ('Accept-Encoding', 'compress'),
            ('Accept-Encoding', 'gzip')],
            kind='client')
    request_parameters = ('encode_multipart', 'max_redirects', 'decompress',
                          'allow_redirects', 'multipart_boundary', 'version',
                          'timeout', 'websocket_handler')
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxy_info dictionary
    no_proxy = set(('localhost', urllibr.localhost(), platform.node()))

    def setup(self, proxy_info=None, cache=None, headers=None,
              encode_multipart=True, multipart_boundary=None,
              key_file=None, cert_file=None, cert_reqs='CERT_NONE',
              ca_certs=None, cookies=None, store_cookies=True,
              max_redirects=10, decompress=True, version=None,
              websocket_handler=None):
        self.store_cookies = store_cookies
        self.max_redirects = max_redirects
        self.cookies = cookies
        self.decompress = decompress
        self.version = version or self.version
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
        self.websocket_handler = websocket_handler
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
                response=None, **params):
        '''Constructs and sends a request to a remote server.
It returns an :class:`HttpResponse` object.

:param method: request method for the :class:`HttpRequest`.
:param url: URL for the :class:`HttpRequest`.
:param params: a dictionary which specify all the optional parameters for
    the :class:`HttpRequest` constructor.

:rtype: a :class:`HttpResponse` object.
'''
        if response:
            rparams = response.current_request.all_params()
            headers = headers or rparams.pop('headers')
            headers.pop('Cookie', None)
            history = copy(rparams['history']) 
            history.append(response.reset_connection())
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.3.4
            if response.status_code is 303:
                method = 'GET'
                rparams.pop('data')
                rparams.pop('files')
            params.update(rparams)
            params['history'] = history
        else:
            params = self.update_parameters(self.request_parameters, params)
        request = HttpRequest(self, url, method, **params)
        request.headers = self.get_headers(request, headers)
        self.set_proxy(request)
        if self.cookies:
            self.cookies.add_cookie_header(request)
        if cookies:
            if not isinstance(cookies, CookieJar):
                cookies = cookiejar_from_dict(cookies)
            cookies.add_cookie_header(request)
        return self.response(request, response)
    
    def add_basic_authentication(self, username, password):
        '''Add a :class:`HTTPBasicAuth` handler to the *pre_requests* hooks.'''
        self.bind_event('pre_request', HTTPBasicAuth(username, password))
        
    def add_digest_authentication(self, username, password):
        self.bind_event('pre_request', HTTPDigestAuth(username, password))

    def get_headers(self, request, headers):
        '''Returns a :class:`Header` obtained from combining
:attr:`headers` with *headers*. It handles websocket requests.'''
        if request.scheme in ('ws','wss'):
            d = Headers((('Connection', 'Upgrade'),
                         ('Upgrade', 'websocket'),
                         ('Sec-WebSocket-Version', str(max(SUPPORTED_VERSIONS))),
                         ('Sec-WebSocket-Key', self.websocket_key),
                         ('user-agent', self.client_version)),
                         kind='client')
        else:
            d = self.headers.copy()
        if headers:
            d.update(headers)
        return d
    
    def set_proxy(self, request):
        if request.scheme in self.proxy_info:
            hostonly = request.host_only
            no_proxy = [n for n in self.proxy_info.get('no','').split(',') if n]
            if not any(map(hostonly.endswith, no_proxy)):
                p = urlparse(self.proxy_info[request.scheme])
                request.set_proxy(p.scheme, p.netloc)
                
    def build_protocol(self, address, timeout):
        type, address = address[0], address[1:] 
        return create_connection(address, timeout)
    
    def can_reuse_connection(self, connection, response):
        # Reuse connection only if the headers has Connection keep-alive
        if response and response.headers:
            return response.headers.has('connection', 'keep-alive')
        return False
    
    def upgrade(self, protocol):
        '''Upgrade the protocol to another one'''
        upgrade = protocol.headers['upgrade']
        callable = getattr(self, 'upgrade_%s' % upgrade, None)
        if not callable:
            raise pulsar.ProtocolError
        return callable(protocol.connection, protocol)
    
    def upgrade_websocket(self, connection, handshake):
        '''Upgrade the *protocol* to a websocket response. Invoked
by the :meth:`upgrade` method.'''
        return WebSocketResponse(connection, handshake)
    