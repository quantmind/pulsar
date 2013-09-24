from functools import partial

from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.websocket import FrameParser
from pulsar.async.stream import SocketStreamSslTransport
from pulsar.utils.httpurl import REDIRECT_CODES, urlparse, urljoin, requote_uri

from pulsar import PulsarException


class TooManyRedirects(PulsarException):

    def __init__(self, response):
        self.response = response


class WebSocketClient(WebSocketProtocol):

    @property
    def request(self):
        return self.handshake._request

    def __getattr__(self, name):
        if not name.startswith('__'):
            return getattr(self.handshake, name)
        else:
            raise AttributeError("'%s' object has no attribute '%s'" %
                                 (self.__class__.__name__, name))


def handle_redirect(response):
    if (response.status_code in REDIRECT_CODES and
            'location' in response.headers and
            response._request.allow_redirects):
        # put at the end of the pile
        response.bind_event('post_request', _do_redirect)
    return response


def _do_redirect(response):
    request = response.request
    client = request.client
    # done with current response
    url = response.headers.get('location')
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
    history = response._history
    if history and len(history) >= request.max_redirects:
        raise TooManyRedirects(response)
    #
    if response.status_code == 303:
        params = request.inp_params.copy()
        method = 'GET'
        params.pop('data', None)
        params.pop('files', None)
        return client.again(response, method, url, params, True)
    else:
        return client.again(response, url=url, history=True)


def handle_cookies(response):
    headers = response.headers
    request = response.request
    client = request.client
    if client.store_cookies and 'set-cookie' in headers:
        client.cookies.extract_cookies(response, request)
    return response


def handle_100(response):
    '''Handle Except: 100-continue'''
    request = response.request
    if (request.headers.has('expect', '100-continue') and
            response.status_code == 100):
            response.bind_event('on_headers', _write_body)
    return response


def _write_body(response):
    response.request.new_parser()
    response.transport.write(response.request.body)


def handle_101(response):
    '''Websocket upgrade as ``on_headers`` event.'''
    if response.status_code == 101:
        connection = response.connection
        request = response._request
        handler = request.websocket_handler
        parser = FrameParser(kind=1)
        if not handler:
            handler = WS()
        factory = partial(WebSocketClient, response, handler, parser)
        response = connection.upgrade(factory, True)
    return response


class TunnelRequest:
    inp_params = None
    # Don't release connection when tunneling
    release_connection = False
    headers = None
    first_line = 'Tunnel connection'

    def __init__(self, request):
        self.request = request
        self.parser = request.parser
        request.new_parser()

    def __repr__(self):
        return self.first_line
    __str__ = __repr__

    @property
    def key(self):
        return self.request.key

    @property
    def client(self):
        return self.request.client

    def encode(self):
        address = self.request.target_address
        self.first_line = 'CONNECT %s:%s HTTP/1.1\r\n' % address
        self.headers = self.request.tunnel_headers
        return b''.join((self.first_line.encode('ascii'), bytes(self.headers)))


class Tunneling:
    '''A callback for handling proxy tunneling.

    The callable method is added as ``pre_request`` to a :class:`HttpClient`.
    If Tunneling is required, it writes the CONNECT headers and abort
    the writing of the actual request until headers from the proxy server
    are received.
    '''
    def __call__(self, response):
        # the pre_request handler
        request = response._request
        tunnel = request._tunnel
        #
        # We Tunnel if a tunnel server is available
        if tunnel:
            # and if transport is not SSL already
            if not isinstance(response.transport, SocketStreamSslTransport):
                response._request = TunnelRequest(request)
                response.bind_event('post_request', self.post_request)
        # make sure to return the response
        return response

    def post_request(self, response):
        '''Called back once the message is complete.'''
        if response.status_code == 200:
            prev_response = response
            request = prev_response._request.request
            loop = response.event_loop
            loop.remove_reader(response.transport.sock.fileno())
            response = request.client.build_consumer()
            response.chain_event(prev_response, 'post_request')
            # Wraps the socket at the next iteration loop. Important!
            loop.call_soon(self.switch_to_ssl, prev_response, response)
        # make sure to return the response
        return response

    def switch_to_ssl(self, prev_response, response):
        '''Wrap the transport for SSL communication.'''
        loop = prev_response.event_loop
        request = prev_response._request.request
        connection = prev_response._connection
        client = request.client
        transport = connection.transport
        sock = transport.sock
        transport = SocketStreamSslTransport(loop, sock, transport.protocol,
                                             request._ssl, server_side=False,
                                             server_hostname=request._netloc)
        connection._transport = transport
        # pause connection made since it will be called again when the
        # ssl handshake occurs
        connection.silence_event('connection_made')
        client.response(request, response, connection=connection)
