import os
import platform
import logging
import asyncio
from functools import partial
from collections import namedtuple
from io import StringIO, BytesIO
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from http.client import responses
try:
    import ssl
    BaseSSLError = ssl.SSLError
except ImportError:     # pragma    nocover
    ssl = None

    class BaseSSLError(Exception):
        pass

try:
    from certifi import where
    DEFAULT_CA_BUNDLE_PATH = where()
except ImportError:     # pragma    nocover
    DEFAULT_CA_BUNDLE_PATH = None

import pulsar
from pulsar import (AbortRequest, AbstractClient, Pool, Connection,
                    isawaitable, ProtocolConsumer, ensure_future,
                    HttpRequestException, HttpConnectionError, SSLError)
from pulsar.utils import websocket
from pulsar.utils.system import json as _json
from pulsar.utils.pep import to_bytes
from pulsar.utils.structures import mapping_iterator
from pulsar.utils.httpurl import (http_parser, encode_multipart_formdata,
                                  Headers, get_environ_proxies, is_succesful,
                                  get_hostport, cookiejar_from_dict,
                                  host_no_default_port, http_chunks,
                                  parse_options_header,
                                  parse_header_links,
                                  JSON_CONTENT_TYPES)

from .plugins import (handle_cookies, WebSocket, Redirect,
                      Tunneling, TooManyRedirects, start_request,
                      response_content, keep_alive, HTTP11)

from .auth import Auth, HTTPBasicAuth, HTTPDigestAuth
from .oauth import OAuth1, OAuth2
from .stream import HttpStream, StreamConsumedError


__all__ = ['HttpRequest', 'HttpResponse', 'HttpClient', 'HTTPDigestAuth',
           'TooManyRedirects', 'Auth', 'OAuth1', 'OAuth2',
           'HttpStream', 'StreamConsumedError']


scheme_host = namedtuple('scheme_host', 'scheme netloc')
tls_schemes = ('https', 'wss')

LOGGER = logging.getLogger('pulsar.http')
FORM_URL_ENCODED = 'application/x-www-form-urlencoded'
MULTIPART_FORM_DATA = 'multipart/form-data'


def guess_filename(obj):
    """Tries to guess the filename of the given object."""
    name = getattr(obj, 'name', None)
    if name and name[0] != '<' and name[-1] != '>':
        return os.path.basename(name)


def scheme_host_port(url):
    url = urlparse(url)
    host, port = get_hostport(url.scheme, url.netloc)
    return url.scheme, host, port


def is_streamed(data):
    try:
        len(data)
    except TypeError:
        return True
    return False


def split_url_params(params):
    for key, values in mapping_iterator(params):
        if not isinstance(values, (list, tuple)):
            values = (values,)
        for value in values:
            yield key, value


class RequestBase:
    inp_params = None
    release_connection = True
    history = None
    url = None
    scheme = None

    @property
    def unverifiable(self):
        """Unverifiable when a redirect.

        It is a redirect when :attr:`history` has past requests.
        """
        return bool(self.history)

    @property
    def address(self):
        return scheme_host_port(self.url)[1:]

    @property
    def origin_req_host(self):
        """Required by Cookies handlers
        """
        if self.history:
            return self.history[0].request.origin_req_host
        else:
            return scheme_host_port(self.url)[1]

    @property
    def type(self):
        return self.scheme

    @property
    def full_url(self):
        return self.url

    def get_full_url(self):
        """Required by Cookies handlers
        """
        return self.url

    def write_body(self, transport):
        pass


class HttpTunnel(RequestBase):
    first_line = None
    data = None

    def __init__(self, request, url):
        self.status = 0
        self.request = request
        self.url = '%s://%s:%s' % scheme_host_port(url)
        self.parser = request.parser
        request.new_parser()
        self.headers = request.client.tunnel_headers.copy()

    def __repr__(self):
        return 'Tunnel %s' % self.url
    __str__ = __repr__

    @property
    def key(self):
        return self.request.key

    @property
    def client(self):
        return self.request.client

    @property
    def version(self):
        return self.request.version

    def encode(self):
        req = self.request
        self.headers['host'] = req.get_header('host')
        bits = scheme_host_port(req.url)[1:] + (req.version,)
        self.first_line = 'CONNECT %s:%s %s\r\n' % bits
        return b''.join((self.first_line.encode('ascii'), bytes(self.headers)))

    def has_header(self, header_name):
        return header_name in self.headers

    def get_header(self, header_name, default=None):
        return self.headers.get(header_name, default)

    def remove_header(self, header_name):
        self.headers.pop(header_name, None)

    def apply(self, response, handler):
        """Tunnel the connection if needed
        """
        connection = response.connection
        tunnel_status = getattr(connection, '_tunnel_status', 0)
        if not tunnel_status:
            connection._tunnel_status = 1
            response.bind_event('pre_request', handler)
        elif tunnel_status == 1:
            connection._tunnel_status = 2
            response._request = self
            response.bind_event('on_headers', handler.on_headers)


class HttpRequest(RequestBase):
    """An :class:`HttpClient` request for an HTTP resource.

    This class has a similar interface to :class:`urllib.request.Request`.

    :param files: optional dictionary of name, file-like-objects.
    :param allow_redirects: allow the response to follow redirects.

    .. attribute:: method

        The request method

    .. attribute:: version

        HTTP version for this request, usually ``HTTP/1.1``

    .. attribute:: history

        List of past :class:`.HttpResponse` (collected during redirects).

    .. attribute:: wait_continue

        if ``True``, the :class:`HttpRequest` includes the
        ``Expect: 100-Continue`` header.

    .. attribute:: stream

        Allow for streaming body

    """
    _proxy = None
    _ssl = None
    _tunnel = None
    _write_done = False

    def __init__(self, client, url, method, inp_params=None, headers=None,
                 data=None, files=None, json=None, history=None, auth=None,
                 charset=None, max_redirects=10, source_address=None,
                 allow_redirects=False, decompress=True, version=None,
                 wait_continue=False, websocket_handler=None, cookies=None,
                 params=None, stream=False, proxies=None, verify=True,
                 **ignored):
        self.client = client
        self.method = method.upper()
        self.inp_params = inp_params or {}
        self.unredirected_headers = Headers(kind='client')
        self.history = history
        self.wait_continue = wait_continue
        self.max_redirects = max_redirects
        self.allow_redirects = allow_redirects
        self.charset = charset or 'utf-8'
        self.version = version
        self.decompress = decompress
        self.websocket_handler = websocket_handler
        self.source_address = source_address
        self.stream = stream
        self.verify = verify
        self.new_parser()
        if auth and not isinstance(auth, Auth):
            auth = HTTPBasicAuth(*auth)
        self.auth = auth
        self.headers = client._get_headers(headers)
        self.url = self._full_url(url, params)
        self.body = self._encode_body(data, files, json)
        self._set_proxy(proxies, ignored)
        cookies = cookiejar_from_dict(client.cookies, cookies)
        if cookies:
            cookies.add_cookie_header(self)

    @property
    def address(self):
        """``(host, port)`` tuple of the HTTP resource
        """
        return self._tunnel.address if self._tunnel else super().address

    @property
    def ssl(self):
        """Context for TLS connections.

        If this is a tunneled request and the tunnel connection is not yet
        established, it returns ``None``.
        """
        if not self._tunnel:
            return self._ssl

    @property
    def key(self):
        tunnel = self._tunnel.url if self._tunnel else None
        scheme, host, port = scheme_host_port(self._proxy or self.url)
        return scheme, host, port, tunnel, self.verify

    @property
    def proxy(self):
        """Proxy server for this request.
        """
        return self._proxy

    @property
    def tunnel(self):
        """Tunnel for this request.
        """
        return self._tunnel

    def __repr__(self):
        return self.first_line()
    __str__ = __repr__

    def first_line(self):
        p = urlparse(self.url)
        if self._proxy:
            if self.method == 'CONNECT':
                url = p.netloc
            else:
                url = self.url
        else:
            url = urlunparse(('', '', p.path or '/', p.params,
                              p.query, p.fragment))
        return '%s %s %s' % (self.method, url, self.version)

    def new_parser(self):
        self.parser = self.client.http_parser(kind=1,
                                              decompress=self.decompress,
                                              method=self.method)

    def is_chunked(self):
        return self.body and 'content-length' not in self.headers

    def encode(self):
        """The bytes representation of this :class:`HttpRequest`.

        Called by :class:`HttpResponse` when it needs to encode this
        :class:`HttpRequest` before sending it to the HTTP resource.
        """
        # Call body before fist_line in case the query is changes.
        first_line = self.first_line()
        if self.body and self.wait_continue:
            self.headers['expect'] = '100-continue'
        headers = self.headers
        if self.unredirected_headers:
            headers = self.unredirected_headers.copy()
            headers.update(self.headers)
        buffer = [first_line.encode('ascii'), b'\r\n',  bytes(headers)]
        return b''.join(buffer)

    def add_header(self, key, value):
        self.headers[key] = value

    def has_header(self, header_name):
        """Check ``header_name`` is in this request headers.
        """
        return (header_name in self.headers or
                header_name in self.unredirected_headers)

    def get_header(self, header_name, default=None):
        """Retrieve ``header_name`` from this request headers.
        """
        return self.headers.get(
            header_name, self.unredirected_headers.get(header_name, default))

    def remove_header(self, header_name):
        """Remove ``header_name`` from this request.
        """
        val1 = self.headers.pop(header_name, None)
        val2 = self.unredirected_headers.pop(header_name, None)
        return val1 or val2

    def add_unredirected_header(self, header_name, header_value):
        self.unredirected_headers[header_name] = header_value

    def write_body(self, transport):
        assert not self._write_done, 'Body already sent'
        self._write_done = True
        if not self.body:
            return
        if is_streamed(self.body):
            ensure_future(self._write_streamed_data(transport),
                          loop=transport._loop)
        else:
            self._write_body_data(transport, self.body, True)

    # INTERNAL ENCODING METHODS
    def _full_url(self, url, params):
        """Full url of endpoint
        """
        p = urlparse(url)
        if not p.netloc and self.method == 'CONNECT':
            p = urlparse('http://%s' % url)

        params = mapping_iterator(params)
        query = parse_qsl(p.query)
        query.extend(split_url_params(params))
        query = urlencode(query)
        return urlunparse((p.scheme, p.netloc, p.path,
                           p.params, query, p.fragment))

    def _encode_body(self, data, files, json):
        body = None
        if isinstance(data, (str, bytes)):
            if files:
                raise ValueError('data cannot be a string or bytes when '
                                 'files are present')
            body = to_bytes(data, self.charset)
        elif data and is_streamed(data):
            if files:
                raise ValueError('data cannot be an iterator when '
                                 'files are present')
            if 'content-length' not in self.headers:
                self.headers['transfer-encoding'] = 'chunked'
            return data
        elif data or files:
            if files:
                body, content_type = self._encode_files(data, files)
            else:
                body, content_type = self._encode_params(data)
            self.headers['Content-Type'] = content_type
        elif json:
            body = _json.dumps(json).encode(self.charset)
            self.headers['Content-Type'] = 'application/json'

        if body:
            self.headers['content-length'] = str(len(body))

        return body

    def _encode_files(self, data, files):
        fields = []
        for field, val in mapping_iterator(data or ()):
            if (isinstance(val, str) or isinstance(val, bytes) or
                    not hasattr(val, '__iter__')):
                val = [val]
            for v in val:
                if v is not None:
                    if not isinstance(v, bytes):
                        v = str(v)
                    fields.append((field.decode('utf-8') if
                                   isinstance(field, bytes) else field,
                                   v.encode('utf-8') if isinstance(v, str)
                                   else v))
        for (k, v) in mapping_iterator(files):
            # support for explicit filename
            ft = None
            if isinstance(v, (tuple, list)):
                if len(v) == 2:
                    fn, fp = v
                else:
                    fn, fp, ft = v
            else:
                fn = guess_filename(v) or k
                fp = v
            if isinstance(fp, bytes):
                fp = BytesIO(fp)
            elif isinstance(fp, str):
                fp = StringIO(fp)
            if ft:
                new_v = (fn, fp.read(), ft)
            else:
                new_v = (fn, fp.read())
            fields.append((k, new_v))
        #
        return encode_multipart_formdata(fields, charset=self.charset)

    def _encode_params(self, params):
        content_type = self.headers.get('content-type')
        # No content type given, chose one
        if not content_type:
            content_type = FORM_URL_ENCODED

        if hasattr(params, 'read'):
            params = params.read()

        if content_type in JSON_CONTENT_TYPES:
            body = _json.dumps(params)
        elif content_type == FORM_URL_ENCODED:
            body = urlencode(tuple(split_url_params(params)))
        elif content_type == MULTIPART_FORM_DATA:
            body, content_type = encode_multipart_formdata(
                params, charset=self.charset)
        else:
            body = params
        return to_bytes(body, self.charset), content_type

    def _write_body_data(self, transport, data, finish=False):
        if self.is_chunked():
            data = http_chunks(data, finish)
        elif data:
            data = (data,)
        else:
            return
        for chunk in data:
            transport.write(chunk)

    async def _write_streamed_data(self, transport):
        for data in self.body:
            if isawaitable(data):
                data = await data
            self._write_body_data(transport, data)
        self._write_body_data(transport, b'', True)

    # PROXY INTERNALS
    def _set_proxy(self, proxies, ignored):
        url = urlparse(self.url)
        self.unredirected_headers['host'] = host_no_default_port(url.scheme,
                                                                 url.netloc)
        if url.scheme in tls_schemes:
            self._ssl = self.client._ssl_context(verify=self.verify, **ignored)

        request_proxies = self.client.proxies.copy()
        if proxies:
            request_proxies.update(proxies)
        self.proxies = request_proxies
        #
        if url.scheme in request_proxies:
            host, port = get_hostport(url.scheme, url.netloc)
            no_proxy = [n for n in request_proxies.get('no', '').split(',')
                        if n]
            if not any(map(host.endswith, no_proxy)):
                url = request_proxies[url.scheme]
                if not self._ssl:
                    self._proxy = url
                else:
                    self._tunnel = HttpTunnel(self, url)


class HttpResponse(ProtocolConsumer):
    """A :class:`.ProtocolConsumer` for the HTTP client protocol.

    Initialised by a call to the :class:`HttpClient.request` method.

    There are two events you can yield in a coroutine:

    .. attribute:: on_headers

        fired once the response headers are received.

    .. attribute:: on_finished

        Fired once the whole request has finished

    Public API:
    """
    _tunnel_host = None
    _has_proxy = False
    _content = None
    _data_sent = None
    _status_code = None
    _cookies = None
    _raw = None
    request_again = None
    ONE_TIME_EVENTS = ('pre_request', 'on_headers', 'post_request')

    @property
    def parser(self):
        request = self.request
        if request:
            return request.parser

    def __repr__(self):
        return '<Response [%s]>' % (self.status_code or 'None')
    __str__ = __repr__

    @property
    def status_code(self):
        """Numeric status code such as 200, 404 and so forth.

        Available once the :attr:`on_headers` has fired.
        """
        return self._status_code

    @property
    def url(self):
        """The request full url.
        """
        request = self.request
        if request:
            return request.url

    @property
    def history(self):
        request = self.request
        if request:
            return request.history

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
        else:
            return False

    @property
    def ok(self):
        return not self.is_error

    @property
    def cookies(self):
        """Dictionary of cookies set by the server or ``None``.
        """
        return self._cookies

    @property
    def content(self):
        """Content of the response, in bytes
        """
        return response_content(self)

    @property
    def raw(self):
        """A raw asynchronous Http response
        """
        if self._raw is None:
            self._raw = HttpStream(self)
        return self._raw

    @property
    def links(self):
        """Returns the parsed header links of the response, if any
        """
        headers = self.headers or {}
        header = headers.get('link')
        l = {}
        if header:
            links = parse_header_links(header)
            for link in links:
                key = link.get('rel') or link.get('url')
                l[key] = link
        return l

    def recv_body(self):
        """Flush the response body and return it.
        """
        return self.parser.recv_body()

    def get_status(self):
        code = self.status_code
        if code:
            return '%d %s' % (code, responses.get(code, 'Unknown'))

    def text(self, charset=None, errors=None):
        """Decode content as a string.
        """
        data = self.content
        if data is not None:
            return data.decode(charset or 'utf-8', errors or 'strict')
    content_string = text

    def json(self, charset=None):
        """Decode content as a JSON object.
        """
        return _json.loads(self.text(charset))

    def decode_content(self):
        """Return the best possible representation of the response body.
        """
        ct = self.headers.get('content-type')
        if ct:
            ct, options = parse_options_header(ct)
            charset = options.get('charset')
            if ct in JSON_CONTENT_TYPES:
                return self.json(charset)
            elif ct.startswith('text/'):
                return self.text(charset)
            elif ct == FORM_URL_ENCODED:
                return parse_qsl(self.content.decode(charset),
                                 keep_blank_values=True)
        return self.content

    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`, if occured.
        """
        if self.is_error:
            if self.status_code:
                raise HttpRequestException(response=self)
            else:
                raise HttpConnectionError(response=self,
                                          msg=self.on_finished.result.error)

    def info(self):
        """Required by python CookieJar.

        Return :attr:`headers`.
        """
        return self.headers

    # #####################################################################
    # #    PROTOCOL IMPLEMENTATION
    def start_request(self):
        request = self._request
        self.transport.write(request.encode())
        if request.headers.get('expect') != '100-continue':
            self.write_body()

    def data_received(self, data):
        request = self.request
        # request.parser my change (100-continue)
        # Always invoke it via request
        try:
            if request.parser.execute(data, len(data)) == len(data):
                if request.parser.is_headers_complete():
                    status_code = request.parser.get_status_code()
                    if (request.headers.has('expect', '100-continue') and
                            status_code == 100):
                        request.new_parser()
                        self.write_body()
                    else:
                        self._status_code = status_code
                        if not self.event('on_headers').fired():
                            self.fire_event('on_headers')
                        if (not self.event('post_request').fired() and
                                request.parser.is_message_complete()):
                            self.finished()
            else:
                raise pulsar.ProtocolError('%s\n%s' % (self, self.headers))
        except Exception as exc:
            self.finished(exc=exc)

    def write_body(self):
        self.request.write_body(self.transport)


class HttpClient(AbstractClient):
    """A client for HTTP/HTTPS servers.

    It handles pool of asynchronous connections.

    :param pool_size: set the :attr:`pool_size` attribute.
    :param store_cookies: set the :attr:`store_cookies` attribute

    .. attribute:: headers

        Default headers for this :class:`HttpClient`.

        Default: :attr:`DEFAULT_HTTP_HEADERS`.

    .. attribute:: cookies

        Default cookies for this :class:`HttpClient`.

    .. attribute:: store_cookies

        If ``True`` it remebers response cookies and send them back to
        serves.

        Default: ``True``

    .. attribute:: timeout

        Default timeout for requests. If None or 0, no timeout on requests

    .. attribute:: proxies

        Dictionary of proxy servers for this client.

    .. attribute:: pool_size

        The size of a pool of connection for a given host.

    .. attribute:: connection_pools

        Dictionary of connection pools for different hosts

    .. attribute:: DEFAULT_HTTP_HEADERS

        Default headers for this :class:`HttpClient`

    """
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request', 'on_headers',
                         'post_request', 'connection_lost')
    protocol_factory = partial(Connection, HttpResponse)
    max_redirects = 10
    """Maximum number of redirects.

    It can be overwritten on :meth:`request`.
    """
    connection_pool = Pool
    """Connection :class:`.Pool` factory
    """
    client_version = pulsar.SERVER_SOFTWARE
    """String for the ``User-Agent`` header.
    """
    version = HTTP11
    """Default HTTP request version for this :class:`HttpClient`.

    It can be overwritten on :meth:`request`.
    """
    DEFAULT_HTTP_HEADERS = Headers((
        ('Connection', 'Keep-Alive'),
        ('Accept', '*/*'),
        ('Accept-Encoding', 'deflate'),
        ('Accept-Encoding', 'gzip')),
        kind='client')
    DEFAULT_TUNNEL_HEADERS = Headers((
        ('Connection', 'Keep-Alive'),
        ('Proxy-Connection', 'Keep-Alive')),
        kind='client')
    request_parameters = ('max_redirects', 'decompress',
                          'websocket_handler', 'version',
                          'verify', 'stream')
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxies dictionary
    no_proxy = set(('localhost', platform.node()))

    def __init__(self, proxies=None, headers=None, verify=True,
                 cookies=None, store_cookies=True,
                 max_redirects=10, decompress=True, version=None,
                 websocket_handler=None, parser=None, trust_env=True,
                 loop=None, client_version=None, timeout=None, stream=False,
                 pool_size=10, frame_parser=None, logger=None,
                 close_connections=False):
        super().__init__(loop)
        self._logger = logger or LOGGER
        self.client_version = client_version or self.client_version
        self.connection_pools = {}
        self.pool_size = pool_size
        self.trust_env = trust_env
        self.timeout = timeout
        self.store_cookies = store_cookies
        self.max_redirects = max_redirects
        self.cookies = cookiejar_from_dict(cookies)
        self.decompress = decompress
        self.version = version or self.version
        self.verify = verify
        self.stream = stream
        self.close_connections = close_connections
        dheaders = self.DEFAULT_HTTP_HEADERS.copy()
        dheaders['user-agent'] = self.client_version
        if headers:
            dheaders.override(headers)
        self.headers = dheaders
        self.tunnel_headers = self.DEFAULT_TUNNEL_HEADERS.copy()
        self.proxies = dict(proxies or ())
        if not self.proxies and self.trust_env:
            self.proxies = get_environ_proxies()
            if 'no' not in self.proxies:
                self.proxies['no'] = ','.join(self.no_proxy)
        self.websocket_handler = websocket_handler
        self.http_parser = parser or http_parser
        self.frame_parser = frame_parser or websocket.frame_parser
        # Add hooks
        self.bind_event('finish', self._close)
        self.bind_event('pre_request', Tunneling(self._loop))
        self.bind_event('pre_request', WebSocket())
        self.bind_event('on_headers', handle_cookies)
        self.bind_event('post_request', Redirect())

    # API
    def connect(self, address):
        if isinstance(address, tuple):
            address = ':'.join(('%s' % v for v in address))
        return self.request('CONNECT', address)

    def get(self, url, **kwargs):
        """Sends a GET request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        kwargs.setdefault('allow_redirects', True)
        return self.request('GET', url, **kwargs)

    def options(self, url, **kwargs):
        """Sends a OPTIONS request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        kwargs.setdefault('allow_redirects', True)
        return self.request('OPTIONS', url, **kwargs)

    def head(self, url, **kwargs):
        """Sends a HEAD request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('HEAD', url, **kwargs)

    def post(self, url, **kwargs):
        """Sends a POST request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        """Sends a PUT request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        """Sends a PATCH request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('PATCH', url, **kwargs)

    def delete(self, url, **kwargs):
        """Sends a DELETE request and returns a :class:`HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('DELETE', url, **kwargs)

    def request(self, method, url, timeout=None, **params):
        """Constructs and sends a request to a remote server.

        It returns a :class:`.Future` which results in a
        :class:`HttpResponse` object.

        :param method: request method for the :class:`HttpRequest`.
        :param url: URL for the :class:`HttpRequest`.
        :parameter response: optional pre-existing :class:`HttpResponse` which
            starts a new request (for redirects, digest authentication and
            so forth).
        :param params: optional parameters for the :class:`HttpRequest`
            initialisation.

        :rtype: a :class:`.Future`
        """
        assert not self.event('finish').fired(), "Http sessions are closed"
        response = self._request(method, url, **params)
        if timeout is None:
            timeout = self.timeout
        if timeout:
            response = asyncio.wait_for(response, timeout, loop=self._loop)
        if not self._loop.is_running():
            return self._loop.run_until_complete(response)
        else:
            return response

    # INTERNALS
    async def _request(self, method, url, **params):
        nparams = params.copy()
        nparams.update(((name, getattr(self, name)) for name in
                        self.request_parameters if name not in params))
        request = HttpRequest(self, url, method, params, **nparams)
        pool = self.connection_pools.get(request.key)
        if pool is None:
            host, port = request.address
            pool = self.connection_pool(
                lambda: self.create_connection((host, port), ssl=request.ssl),
                pool_size=self.pool_size, loop=self._loop)
            self.connection_pools[request.key] = pool
        try:
            conn = await pool.connect()
        except BaseSSLError as e:
            raise SSLError(str(e), response=self) from None
        except ConnectionRefusedError as e:
            raise HttpConnectionError(str(e), response=self) from None

        with conn:
            try:
                response = await start_request(request, conn)
                headers = response.headers
            except AbortRequest:
                response = None
                headers = None

            if (not headers or
                    not keep_alive(response.request.version, headers) or
                    response.status_code == 101 or
                    response.request.stream or
                    self.close_connections):
                conn.detach()

        # Handle a possible redirect
        if response and isinstance(response.request_again, tuple):
            method, url, params = response.request_again
            response = await self._request(method, url, **params)
        return response

    def _get_headers(self, headers=None):
        # Returns a :class:`Header` obtained from combining
        # :attr:`headers` with *headers*. Can handle websocket requests.
        d = self.headers.copy()
        if headers:
            d.override(headers)
        return d

    def _ssl_context(self, verify=True, cert_reqs=None,
                     check_hostname=False, certfile=None, keyfile=None,
                     cafile=None, capath=None, cadata=None, **kw):
        assert ssl, 'SSL not supported'
        cafile = cafile or DEFAULT_CA_BUNDLE_PATH

        if verify is True:
            cert_reqs = ssl.CERT_REQUIRED
            check_hostname = True

        if isinstance(verify, str):
            cert_reqs = ssl.CERT_REQUIRED
            if os.path.isfile(verify):
                cafile = verify
            elif os.path.isdir(verify):
                capath = verify

        return ssl._create_unverified_context(cert_reqs=cert_reqs,
                                              check_hostname=check_hostname,
                                              certfile=certfile,
                                              keyfile=keyfile,
                                              cafile=cafile,
                                              capath=capath,
                                              cadata=cadata)

    def _close(self, _, exc=None):
        # Internal method, use self.close()
        # Close all connections.
        # Returns a future which can be used to wait for complete closure
        waiters = []
        for p in self.connection_pools.values():
            waiters.append(p.close())
        self.connection_pools.clear()
        return asyncio.gather(*waiters, loop=self._loop)
