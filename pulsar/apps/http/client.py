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

from multidict import CIMultiDict

import pulsar
from pulsar.api import (
    AbortEvent, AbstractClient, Pool, Connection,
    ProtocolConsumer, HttpRequestException, HttpConnectionError,
    SSLError, cfg_value
)
from pulsar.utils import websocket
from pulsar.utils.system import json as _json
from pulsar.utils.string import to_bytes
from pulsar.utils import http
from pulsar.utils.structures import mapping_iterator
from pulsar.async.timeout import timeout as async_timeout
from pulsar.utils.httpurl import (
    encode_multipart_formdata, CHARSET, get_environ_proxies, is_succesful,
    get_hostport, cookiejar_from_dict, http_chunks, JSON_CONTENT_TYPES,
    parse_options_header, tls_schemes, parse_header_links, requote_uri,
)

from .plugins import (
    handle_cookies, WebSocket, Redirect, start_request, RequestKey,
    keep_alive, InfoHeaders, Expect
)
from .auth import Auth, HTTPBasicAuth
from .stream import HttpStream
from .decompress import GzipDecompress, DeflateDecompress


scheme_host = namedtuple('scheme_host', 'scheme netloc')

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


def full_url(url, params, method=None):
    p = urlparse(url)
    if not p.netloc and method == 'CONNECT':
        p = urlparse('http://%s' % url)

    params = mapping_iterator(params)
    query = parse_qsl(p.query, True)
    query.extend(split_url_params(params))
    query = urlencode(query)
    return requote_uri(
        urlunparse((p.scheme, p.netloc, p.path, p.params, query, p.fragment))
    )


class RequestBase:
    inp_params = None
    release_connection = True
    history = None
    url = None

    @property
    def unverifiable(self):
        """Unverifiable when a redirect.

        It is a redirect when :attr:`history` has past requests.
        """
        return bool(self.history)

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
        return self.key.scheme

    @property
    def full_url(self):
        return self.url

    def new_parser(self, protocol):
        protocol.headers = CIMultiDict()
        return self.client.http_parser(protocol)

    def get_full_url(self):
        """Required by Cookies handlers
        """
        return self.url

    def write_body(self, transport):
        pass


class HttpTunnel(RequestBase):
    first_line = None
    data = None
    decompress = False
    method = 'CONNECT'

    def __init__(self, client, req):
        self.client = client
        self.key = req
        self.headers = CIMultiDict(client.DEFAULT_TUNNEL_HEADERS)

    def __repr__(self):
        return 'Tunnel %s' % self.url
    __str__ = __repr__

    def encode(self):
        self.headers['host'] = self.key.netloc
        self.first_line = 'CONNECT http://%s:%s HTTP/1.1' % self.key.address
        buffer = [self.first_line.encode('ascii'), b'\r\n']
        buffer.extend((('%s: %s\r\n' % (name, value)).encode(CHARSET)
                       for name, value in self.headers.items()))
        buffer.append(b'\r\n')
        return b''.join(buffer)

    def has_header(self, header_name):
        return header_name in self.headers

    def get_header(self, header_name, default=None):
        return self.headers.get(header_name, default)

    def remove_header(self, header_name):
        self.headers.pop(header_name, None)


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
                 cert=None, **extra):
        self.client = client
        self.method = method.upper()
        self.inp_params = inp_params or {}
        self.unredirected_headers = CIMultiDict()
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
        self.cert = cert
        if auth and not isinstance(auth, Auth):
            auth = HTTPBasicAuth(*auth)
        self.auth = auth
        self.url = full_url(url, params, method=self.method)
        self._set_proxy(proxies)
        self.key = RequestKey.create(self)
        self.headers = client.get_headers(self, headers)
        self.body = self._encode_body(data, files, json)
        self.unredirected_headers['host'] = self.key.netloc
        cookies = cookiejar_from_dict(client.cookies, cookies)
        if cookies:
            cookies.add_cookie_header(self)

    @property
    def _loop(self):
        return self.client._loop

    @property
    def ssl(self):
        """Context for TLS connections.

        If this is a tunneled request and the tunnel connection is not yet
        established, it returns ``None``.
        """
        return self._ssl

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
        if self.method == 'CONNECT':
            url = self.key.netloc
        elif self._proxy:
            url = self.url
        else:
            p = urlparse(self.url)
            url = urlunparse(('', '', p.path or '/', p.params,
                              p.query, p.fragment))
        return '%s %s %s' % (self.method, url, self.version)

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
        buffer = [first_line.encode('ascii'), b'\r\n']
        buffer.extend((('%s: %s\r\n' % (name, value)).encode(CHARSET)
                      for name, value in headers.items()))
        buffer.append(b'\r\n')
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
            self._loop.create_task(self._write_streamed_data(transport))
        else:
            self._write_body_data(transport, self.body, True)

    # INTERNAL ENCODING METHODS
    def _encode_body(self, data, files, json):
        body = None
        ct = None
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
                body, ct = self._encode_files(data, files)
            else:
                body, ct = self._encode_params(data)
        elif json:
            body = _json.dumps(json).encode(self.charset)
            ct = 'application/json'

        if not self.headers.get('content-type') and ct:
            self.headers['Content-Type'] = ct

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
            try:
                data = await data
            except TypeError:
                pass
            self._write_body_data(transport, data)
        self._write_body_data(transport, b'', True)

    # PROXY INTERNALS
    def _set_proxy(self, proxies):
        url = urlparse(self.url)
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
                proxy_url = request_proxies[url.scheme]
                if url.scheme in tls_schemes:
                    self._tunnel = proxy_url
                else:
                    self._proxy = proxy_url


class HttpResponse(ProtocolConsumer):
    """A :class:`.ProtocolConsumer` for the HTTP client protocol.

    Initialised by a call to the :class:`HttpClient.request` method.
    """
    _has_proxy = False
    _data_sent = None
    _cookies = None
    _raw = None
    content = None
    headers = None
    parser = None
    version = None
    status_code = None
    request_again = None
    ONE_TIME_EVENTS = ('pre_request', 'on_headers', 'post_request')

    def __repr__(self):
        return '<Response [%s]>' % (self.status_code or 'None')
    __str__ = __repr__

    @property
    def url(self):
        """The request full url.
        """
        request = self.request
        if request:
            return request.url

    @property
    def history(self):
        """List of :class:`.HttpResponse` objects from the history of the
        request. Any redirect responses will end up here.
        The list is sorted from the oldest to the most recent request."""
        request = self.request
        if request:
            return request.history

    @property
    def ok(self):
        if self.status_code:
            return is_succesful(self.status_code)
        else:
            return not self.event('post_request').fired()

    @property
    def cookies(self):
        """Dictionary of cookies set by the server or ``None``.
        """
        return self._cookies

    @property
    def encoding(self):
        ct = self.headers.get('content-type')
        if ct:
            ct, options = parse_options_header(ct)
            return options.get('charset')

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
        li = {}
        if header:
            links = parse_header_links(header)
            for link in links:
                key = link.get('rel') or link.get('url')
                li[key] = link
        return li

    @property
    def reason(self):
        return responses.get(self.status_code)

    @property
    def text(self):
        """Decode content as a string.
        """
        data = self.content
        return data.decode(self.encoding or 'utf-8') if data else ''

    def json(self):
        """Decode content as a JSON object.
        """
        return _json.loads(self.text)

    def decode_content(self):
        """Return the best possible representation of the response body.
        """
        ct = self.headers.get('content-type')
        if ct:
            ct, options = parse_options_header(ct)
            charset = options.get('charset')
            if ct in JSON_CONTENT_TYPES:
                return self.json()
            elif ct.startswith('text/'):
                return self.text
            elif ct == FORM_URL_ENCODED:
                return parse_qsl(self.content.decode(charset),
                                 keep_blank_values=True)
        return self.content

    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`, if occurred.
        """
        if not self.ok:
            reason = self.reason or 'No response from %s' % self.url
            if not self.status_code:
                raise HttpConnectionError(reason, response=self)

            if 400 <= self.status_code < 500:
                http_error_msg = '%s Client Error - %s - %s %s' % (
                    self.status_code, reason, self.request.method, self.url)
            else:
                http_error_msg = '%s Server Error - %s - %s %s' % (
                    self.status_code, reason, self.request.method, self.url)

            raise HttpRequestException(http_error_msg, response=self)

    def info(self):
        """Required by python CookieJar.

        Return :attr:`headers`.
        """
        return InfoHeaders(self.headers)

    # #####################################################################
    # #    PROTOCOL CONSUMER IMPLEMENTATION
    def start_request(self):
        request = self.request
        self.parser = request.new_parser(self)
        headers = request.encode()
        self.connection.transport.write(headers)
        if not headers or request.headers.get('expect') != '100-continue':
            self.write_body()

    def feed_data(self, data):
        try:
            self.parser.feed_data(data)
        except http.HttpParserUpgrade:
            pass

    def on_header(self, name, value):
        self.headers.add(name.decode(CHARSET), value.decode(CHARSET))

    def on_headers_complete(self):
        request = self.request
        self.status_code = self.parser.get_status_code()
        self.version = self.parser.get_http_version()
        self.event('on_headers').fire()
        if request.method == 'HEAD':
            self.event('post_request').fire()

    def on_body(self, body):
        if self.request.stream or self._raw:
            self.raw.feed_data(body)
        elif self.content is None:
            self.content = body
        else:
            self.content += body

    def on_message_complete(self):
        self.producer.maybe_decompress(self)
        self.fire_event('post_request')

    def write_body(self):
        self.request.write_body(self.connection)


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

        If ``True`` it remembers response cookies and sends them back to
        servers.

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
    version = 'HTTP/1.1'
    """Default HTTP request version for this :class:`HttpClient`.

    It can be overwritten on :meth:`request`.
    """
    DEFAULT_HTTP_HEADERS = (
        ('Connection', 'Keep-Alive'),
        ('Accept', '*/*'),
        ('Accept-Encoding', 'deflate'),
        ('Accept-Encoding', 'gzip')
    )
    DEFAULT_TUNNEL_HEADERS = (
        ('Connection', 'Keep-Alive'),
        ('Proxy-Connection', 'Keep-Alive')
    )
    request_parameters = (
        'max_redirects',
        'decompress',
        'websocket_handler',
        'version',
        'verify',
        'stream',
        'cert'
    )
    # Default hosts not affected by proxy settings. This can be overwritten
    # by specifying the "no" key in the proxies dictionary
    no_proxy = set(('localhost', platform.node()))

    def __init__(self, proxies=None, headers=None, verify=True,
                 cookies=None, store_cookies=True, cert=None,
                 max_redirects=10, decompress=True, version=None,
                 websocket_handler=None, parser=None, trust_env=True,
                 loop=None, client_version=None, timeout=None, stream=False,
                 pool_size=10, frame_parser=None, logger=None,
                 close_connections=False, keep_alive=None):
        super().__init__(
            partial(Connection, HttpResponse),
            loop=loop,
            keep_alive=keep_alive or cfg_value('http_keep_alive')
        )
        self.logger = logger or LOGGER
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
        # SSL Verification default
        self.verify = verify
        # SSL client certificate default, if String, path to ssl client
        # cert file (.pem). If Tuple, ('cert', 'key') pair
        self.cert = cert
        self.stream = stream
        self.close_connections = close_connections
        dheaders = CIMultiDict(self.DEFAULT_HTTP_HEADERS)
        dheaders['user-agent'] = self.client_version
        # override headers
        if headers:
            for name, value in mapping_iterator(headers):
                if value is None:
                    dheaders.pop(name, None)
                else:
                    dheaders[name] = value
        self.headers = dheaders
        self.proxies = dict(proxies or ())
        if not self.proxies and self.trust_env:
            self.proxies = get_environ_proxies()
            if 'no' not in self.proxies:
                self.proxies['no'] = ','.join(self.no_proxy)
        self.websocket_handler = websocket_handler
        self.http_parser = parser or http.HttpResponseParser
        self.frame_parser = frame_parser or websocket.frame_parser
        # Add hooks
        self.event('on_headers').bind(handle_cookies)
        self.event('pre_request').bind(WebSocket())
        self.event('post_request').bind(Expect())
        self.event('post_request').bind(Redirect())
        self._decompressors = dict(
            gzip=GzipDecompress(),
            deflate=DeflateDecompress()
        )

    # API
    def connect(self, address):
        if isinstance(address, tuple):
            address = ':'.join(('%s' % v for v in address))
        return self.request('CONNECT', address)

    def get(self, url, **kwargs):
        """Sends a GET request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('GET', url, **kwargs)

    def options(self, url, **kwargs):
        """Sends a OPTIONS request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('OPTIONS', url, **kwargs)

    def head(self, url, **kwargs):
        """Sends a HEAD request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('HEAD', url, **kwargs)

    def post(self, url, **kwargs):
        """Sends a POST request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        """Sends a PUT request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        """Sends a PATCH request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('PATCH', url, **kwargs)

    def delete(self, url, **kwargs):
        """Sends a DELETE request and returns a :class:`.HttpResponse` object.

        :params url: url for the new :class:`HttpRequest` object.
        :param \*\*kwargs: Optional arguments for the :meth:`request` method.
        """
        return self.request('DELETE', url, **kwargs)

    def request(self, method, url, **params):
        """Constructs and sends a request to a remote server.

        It returns a :class:`.Future` which results in a
        :class:`.HttpResponse` object.

        :param method: request method for the :class:`HttpRequest`.
        :param url: URL for the :class:`HttpRequest`.
        :param params: optional parameters for the :class:`HttpRequest`
            initialisation.

        :rtype: a coroutine
        """
        response = self._request(method, url, **params)
        if not self._loop.is_running():
            return self._loop.run_until_complete(response)
        else:
            return response

    def close(self):
        """Close all connections
        """
        waiters = []
        for p in self.connection_pools.values():
            waiters.append(p.close())
        self.connection_pools.clear()
        return asyncio.gather(*waiters, loop=self._loop)

    def maybe_decompress(self, response):
        encoding = response.headers.get('content-encoding')
        if encoding and response.request.decompress:
            deco = self._decompressors.get(encoding)
            if not deco:
                self.logger.warning('Cannot decompress %s', encoding)
            response.content = deco(response.content)

    async def __aenter__(self):
        await self.close()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # INTERNALS
    async def _request(self, method, url, timeout=None, **params):
        if timeout is None:
            timeout = self.timeout

        if method != 'HEAD':
            params.setdefault('allow_redirects', True)

        with async_timeout(self._loop, timeout):
            nparams = params.copy()
            nparams.update(((name, getattr(self, name)) for name in
                            self.request_parameters if name not in params))
            request = HttpRequest(self, url, method, params, **nparams)
            key = request.key
            pool = self.connection_pools.get(key)
            if pool is None:
                tunnel = request.tunnel
                if tunnel:
                    connector = partial(self.create_tunnel_connection, key)
                else:
                    connector = partial(self.create_http_connection, key)
                pool = self.connection_pool(
                    connector, pool_size=self.pool_size, loop=self._loop
                )
                self.connection_pools[request.key] = pool
            try:
                conn = await pool.connect()
            except BaseSSLError as e:
                raise SSLError(str(e), response=self) from None
            except ConnectionRefusedError as e:
                raise HttpConnectionError(str(e), response=self) from None

            async with conn:
                try:
                    response = await start_request(request, conn)
                    status_code = response.status_code
                except AbortEvent:
                    response = None
                    status_code = None

                if (not status_code or
                        not keep_alive(response.version, response.headers) or
                        status_code == 101 or
                        # if response is done stream is not relevant
                        (response.request.stream and not
                         response.event('post_request').fired()) or
                        self.close_connections):
                    await conn.detach()

            # Handle a possible redirect
            if response and isinstance(response.request_again, tuple):
                method, url, params = response.request_again
                response = await self._request(method, url, **params)
            return response

    def get_headers(self, request, headers):
        # Returns a :class:`Header` obtained from combining
        # :attr:`headers` with *headers*. Can handle websocket requests.
        # TODO: this is a buf in CIMultiDict
        # d = self.headers.copy()
        d = CIMultiDict(self.headers.items())
        if headers:
            d.update(headers)
        return d

    def ssl_context(self, verify=True, cert_reqs=None,
                    check_hostname=False, certfile=None, keyfile=None,
                    cafile=None, capath=None, cadata=None, **kw):
        """Create a SSL context object.

        This method should not be called by from user code
        """
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

    async def create_http_connection(self, req):
        return await self.create_connection(req.address, ssl=req.ssl(self))

    async def create_tunnel_connection(self, req):
        """Create a tunnel connection
        """
        tunnel_address = req.tunnel_address
        connection = await self.create_connection(tunnel_address)
        response = connection.current_consumer()
        for event in response.events().values():
            event.clear()
        response.start(HttpTunnel(self, req))
        await response.event('post_request').waiter()
        if response.status_code != 200:
            raise ConnectionRefusedError(
                'Cannot connect to tunnel: status code %s'
                % response.status_code
            )
        raw_sock = connection.transport.get_extra_info('socket')
        if raw_sock is None:
            raise RuntimeError('Transport without socket')
        # duplicate socket so we can close transport
        raw_sock = raw_sock.dup()
        connection.transport.close()
        await connection.event('connection_lost').waiter()
        self.sessions -= 1
        self.requests_processed -= 1
        #
        connection = await self.create_connection(
            sock=raw_sock, ssl=req.ssl(self), server_hostname=req.netloc
        )
        return connection
