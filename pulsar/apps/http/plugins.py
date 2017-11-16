from functools import partial
from collections import namedtuple
from copy import copy
from http.cookies import SimpleCookie
from urllib.parse import urlparse, urljoin

from pulsar.api import PulsarException
from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.httpurl import (
    REDIRECT_CODES, requote_uri, get_hostport, host_no_default_port,
    tls_schemes
)
from pulsar.utils.websocket import SUPPORTED_VERSIONS, websocket_key


requestKey = namedtuple('requestKey', 'scheme host port tunnel verify cert')


class RequestKey(requestKey):

    @classmethod
    def create(cls, request):
        url = urlparse(request.proxy or request.url)
        host, port = get_hostport(url.scheme, url.netloc)
        return cls(
            url.scheme, host, port,
            request.tunnel,
            request.verify, request.cert
        )

    @property
    def address(self):
        return self.host, self.port

    @property
    def netloc(self):
        return host_no_default_port(self.scheme, '%s:%s' % self.address)

    @property
    def tunnel_address(self):
        if self.tunnel:
            url = urlparse(self.tunnel)
            return get_hostport(url.scheme, url.netloc)

    def ssl(self, client):
        if self.scheme in tls_schemes:
            if isinstance(self.cert, tuple):
                certfile, keyfile = self.cert
            else:
                certfile, keyfile = self.cert, None
            return client.ssl_context(
                verify=self.verify,
                certfile=certfile,
                keyfile=keyfile
            )


def keep_alive(version, headers):
    """Check if to keep alive an HTTP connection.

    If the version is 1.1, we close the connection only if the ``connection``
    header is available and set to ``close``
    """
    if version == '1.1':
        return not headers.get('connection') == 'close'
    else:
        return headers.get('connection') == 'keep-alive'


async def start_request(request, conn):
    response = conn.current_consumer()

    # bind request-specific events
    response.bind_events(request.inp_params)
    if request.auth:
        response.event('pre_request').bind(request.auth)

    response.start(request)
    if request.stream:
        await response.event('on_headers').waiter()
    else:
        await response.event('post_request').waiter()

    if hasattr(response.request_again, '__call__'):
        response = response.request_again(response)
        try:
            response = await response
        except TypeError:
            pass

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
    def request(self):
        return self.handshake.request

    @property
    def headers(self):
        return self.handshake.headers

    def raise_for_status(self):
        pass

    def __getattr__(self, name):
        if not name.startswith('__'):
            return getattr(self.handshake, name)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (self.__class__.__name__, name))


class Expect:

    def __call__(self, response, exc=None):
        if response.status_code == 100:
            expect = response.request.headers.get('expect')
            if expect and expect.lower() == '100-continue':
                response.request_again = self._response

    def _response(self, response):
        request = response.request
        request.encode = self.empty
        return start_request(request, response.connection)

    def empty(self):
        return b''


class Redirect:

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


def handle_cookies(response, exc=None):
    '''Handle response cookies.
    '''
    if exc:
        return
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

    def __call__(self, response, exc=None):
        request = response.request
        if (not exc and request and
                urlparse(request.url).scheme in ('ws', 'wss')):
            headers = request.headers
            headers['connection'] = 'Upgrade'
            headers['upgrade'] = 'websocket'
            if 'Sec-WebSocket-Version' not in headers:
                headers['Sec-WebSocket-Version'] = str(max(SUPPORTED_VERSIONS))
            if 'Sec-WebSocket-Key' not in headers:
                headers['Sec-WebSocket-Key'] = self.websocket_key
            response.event('on_headers').bind(self.on_headers)

    def on_headers(self, response, exc=None):
        '''Websocket upgrade as ``on_headers`` event.'''

        if response.status_code == 101:
            connection = response.connection
            request = response.request
            handler = request.websocket_handler
            if not handler:
                handler = WS()
            parser = request.client.frame_parser(kind=1)
            consumer = partial(WebSocketClient.create,
                               response, handler, parser)
            connection.upgrade(consumer)
            response.event('post_request').fire()
            websocket = connection.current_consumer()
            response.request_again = lambda r: websocket


class InfoHeaders:
    __slots__ = ('headers',)

    def __init__(self, headers):
        self.headers = headers

    def get_all(self, key, default=None):
        return self.headers.getall(key, default)
