import asyncio
from functools import partial
from collections import namedtuple
from copy import copy
from urllib.parse import urlparse, urljoin

from pulsar import OneTime, isawaitable
from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.httpurl import REDIRECT_CODES, requote_uri, SimpleCookie
from pulsar.utils.websocket import SUPPORTED_VERSIONS, websocket_key
from pulsar import PulsarException


def noerror(callback):
    '''Decorator to run a callback of a :class:`.EventHandler`
    only when no errors occur
    '''
    def _(*response, **kw):
        if response[-1] and not kw.get('exc'):
            return callback(*response)

    return _


HTTP11 = 'HTTP/1.1'


def keep_alive(version, headers):
    """Check if to keep alive an HTTP connection.

    If the version is 1.1, we close the connection only if the ``connection``
    header is available and set to ``close``
    """
    if version == HTTP11:
        return not headers.has('connection', 'close')
    else:
        return headers.has('connection', 'keep-alive')


def response_content(resp, exc=None, **kw):
    b = resp.parser.recv_body()
    if b or resp._content is None:
        resp._content = resp._content + b if resp._content else b
    return resp._content


def _consumer(response, consumer):
    if response is not None:
        consumer = response
    return consumer


async def start_request(request, conn):
    response = conn.current_consumer()
    # bind request-specific events
    response.bind_events(**request.inp_params)
    if request.auth:
        response.bind_event('pre_request', request.auth)

    if request.stream:
        response.bind_event('data_processed', response.raw)
        response.start(request)
        await response.events['on_headers']
    else:
        response.bind_event('data_processed', response_content)
        response.start(request)
        await response.on_finished

    if hasattr(response.request_again, '__call__'):
        response = response.request_again(response)
        if isawaitable(response):
            response = await response

    return response


class request_again(namedtuple('request_again', 'method url params')):

    @property
    def status_code(self):
        return -1

    @property
    def headers(self):
        return ()


class TooManyRedirects(PulsarException):

    def __init__(self, response):
        self.response = response


class WebSocketClient(WebSocketProtocol):
    status_code = 101

    @property
    def _request(self):
        return self.handshake._request

    @property
    def headers(self):
        return self.handshake.headers

    def __getattr__(self, name):
        if not name.startswith('__'):
            return getattr(self.handshake, name)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (self.__class__.__name__, name))


class Redirect:

    @noerror
    def __call__(self, response, exc=None):
        if (response.status_code in REDIRECT_CODES and
                'location' in response.headers and
                response.request.allow_redirects):
            response.request_again = self._do_redirect

    def _do_redirect(self, response):
        request = response.request
        # done with current response
        url = response.headers.get('location')
        # Handle redirection without scheme (see: RFC 1808 Section 4)
        if url.startswith('//'):
            parsed_rurl = urlparse(request.url)
            url = '%s:%s' % (parsed_rurl.scheme, url)
        # Facilitate non-RFC2616-compliant 'location' headers
        # (e.g. '/path/to/resource' instead of
        # 'http://domain.tld/path/to/resource')
        if not urlparse(url).netloc:
            url = urljoin(request.url,
                          # Compliant with RFC3986, we percent
                          # encode the url.
                          requote_uri(url))
        history = request.history
        if history and len(history) >= request.max_redirects:
            raise TooManyRedirects(response)

        params = request.inp_params.copy()
        params['history'] = copy(history) if history else []
        params['history'].append(response)
        if response.status_code == 303:
            method = 'GET'
            params.pop('data', None)
            params.pop('files', None)
        else:
            method = request.method
        response.request_again = request_again(method, url, params)
        return response


@noerror
def handle_cookies(response, exc=None):
    '''Handle response cookies.
    '''
    headers = response.headers
    request = response.request
    client = request.client
    response._cookies = c = SimpleCookie()
    if 'set-cookie' in headers or 'set-cookie2' in headers:
        for cookie in (headers.get('set-cookie2'),
                       headers.get('set-cookie')):
            if cookie:
                c.load(cookie)
        if client.store_cookies:
            client.cookies.extract_cookies(response, request)


class WebSocket:

    @property
    def websocket_key(self):
        if not hasattr(self, '_websocket_key'):
            self._websocket_key = websocket_key()
        return self._websocket_key

    @noerror
    def __call__(self, response, exc=None):
        request = response.request
        if request and urlparse(request.url).scheme in ('ws', 'wss'):
            headers = request.headers
            headers['connection'] = 'Upgrade'
            headers['upgrade'] = 'websocket'
            if 'Sec-WebSocket-Version' not in headers:
                headers['Sec-WebSocket-Version'] = str(max(SUPPORTED_VERSIONS))
            if 'Sec-WebSocket-Key' not in headers:
                headers['Sec-WebSocket-Key'] = self.websocket_key
            response.bind_event('on_headers', self.on_headers)

    @noerror
    def on_headers(self, response, exc=None):
        '''Websocket upgrade as ``on_headers`` event.'''

        if response.status_code == 101:
            connection = response.connection
            request = response.request
            handler = request.websocket_handler
            if not handler:
                handler = WS()
            parser = request.client.frame_parser(kind=1)
            consumer = partial(WebSocketClient, response, handler, parser)
            connection.upgrade(consumer)
            body = response.recv_body()
            response.finished()
            websocket = connection.current_consumer()
            websocket.data_received(body)
            response.request_again = lambda r: websocket


class Tunneling:
    '''A pre request callback for handling proxy tunneling.

    If Tunnelling is required, it writes the CONNECT headers and abort
    the writing of the actual request until headers from the proxy server
    are received.
    '''
    def __init__(self, loop):
        assert loop
        self._loop = loop

    @noerror
    def __call__(self, response, exc=None):
        # the pre_request handler
        request = response.request
        if request and request.tunnel:
            request.tunnel.apply(response, self)

    @noerror
    def on_headers(self, response, exc=None):
        '''Called back once the headers have arrived.'''
        if response.status_code == 200:
            response.request_again = self._tunnel_request
            response.finished()

    async def _tunnel_request(self, response):
        request = response.request.request
        connection = response.connection
        loop = connection._loop
        sock = connection.sock
        connection.transport.pause_reading()
        # await asyncio.sleep(0.01)
        # set a new connection_made event
        connection.events['connection_made'] = OneTime(loop=loop)
        connection._processed -= 1
        connection.producer._requests_processed -= 1
        #
        # For some reason when using the code below, it fails in python 3.5
        # _, connection = await loop._create_connection_transport(
        #         sock, lambda: connection,
        #         request._ssl,
        #         server_hostname=request._netloc)
        #
        # Therefore use legacy SSL transport
        waiter = asyncio.Future(loop=loop)
        url = urlparse(request.url)
        loop._make_legacy_ssl_transport(sock, connection,
                                        request._ssl, waiter,
                                        server_hostname=url.netloc)
        await waiter
        #
        await connection.event('connection_made')
        response = await start_request(request, connection)
        return response
