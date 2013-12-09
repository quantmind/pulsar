from functools import partial
from collections import namedtuple
from copy import copy

from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.websocket import frame_parser
from pulsar.async.stream import SocketStreamSslTransport
from pulsar.utils.httpurl import (REDIRECT_CODES, urlparse, urljoin,
                                  requote_uri, parse_cookie)

from pulsar import PulsarException


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
    history = request.history
    if history and len(history) >= request.max_redirects:
        raise TooManyRedirects(response)
    #
    params = request.inp_params.copy()
    params['history'] = copy(history) if history else []
    params['history'].append(response)
    if response.status_code == 303:
        method = 'GET'
        params.pop('data', None)
        params.pop('files', None)
    else:
        method = request.method
    return request_again(method, url, params)


def handle_cookies(response):
    headers = response.headers
    request = response.request
    client = request.client
    if 'set-cookie' in headers or 'set-cookie2' in headers:
        response._cookies = cookies = {}
        for cookie in (headers.get('set-cookie2'),
                       headers.get('set-cookie')):
            if cookie:
                cookies.update(parse_cookie(cookie))
        if client.store_cookies:
            client.cookies.extract_cookies(response, request)
    return response


def handle_100(response):
    '''Handle Except: 100-continue.

    This is a pre_request hook which checks if the request headers
    have the ``Expect: 100-continue`` value. If so add a ``on_headers``
    callback to handle the response from the server.
    '''
    request = response.request
    if (request.headers.has('expect', '100-continue') and
            response.status_code == 100):
            response.bind_event('on_headers', _write_body)
    return response


def _write_body(response):
    if response.status_code == 100:
        response.request.new_parser()
        if response.request.body:
            response.transport.write(response.request.body)
    return response


def handle_101(response):
    '''Websocket upgrade as ``on_headers`` event.'''
    if response.status_code == 101:
        connection = response.connection
        request = response._request
        handler = request.websocket_handler
        parser = frame_parser(kind=1)
        if not handler:
            handler = WS()
        connection.upgrade(partial(WebSocketClient, response, handler, parser))
        response.finished()
    return response


class Tunneling:
    '''A pre request callback for handling proxy tunneling.

    If Tunnelling is required, it writes the CONNECT headers and abort
    the writing of the actual request until headers from the proxy server
    are received.
    '''
    def __call__(self, response):
        # the pre_request handler
        request = response._request
        if request:
            tunnel = request._tunnel
            if tunnel:
                if getattr(request, '_apply_tunnel', False):
                    # if transport is not SSL already
                    if not isinstance(response.transport,
                                      SocketStreamSslTransport):
                        response._request = tunnel
                        response.bind_event('on_headers', self.on_headers)
                else:
                    # Append self again as pre_request
                    request._apply_tunnel = True
                    response.bind_event('pre_request', self)
        # make sure to return the response
        return response

    def on_headers(self, response):
        '''Called back once the headers have arrived.'''
        if response.status_code == 200:
            loop = response._loop
            loop.remove_reader(response.transport.sock.fileno())
            # Wraps the socket at the next iteration loop. Important!
            loop.call_soon_threadsafe(self.switch_to_ssl, response)
        # make sure to return the response
        return response

    def switch_to_ssl(self, prev_response):
        '''Wrap the transport for SSL communication.'''
        loop = prev_response._loop
        request = prev_response._request.request
        connection = prev_response._connection
        connection.upgrade(connection._consumer_factory)
        transport = connection.transport
        sock = transport.sock
        transport = SocketStreamSslTransport(loop, sock, transport.protocol,
                                             request._ssl, server_side=False,
                                             server_hostname=request._netloc)
        connection._transport = transport
        # silence connection made since it will be called again when the
        # ssl handshake occurs. This is just to avoid unwanted logging.
        #
        connection.silence_event('connection_made')
        connection._processed -= 1
        connection.producer._requests_processed -= 1
        #
        prev_response.bind_event('post_request',
                                 partial(self.start_tunneling, request))
        prev_response.finished()

    def start_tunneling(self, request, consumer):
        print('tunneling %s' % request)
        consumer.start(request)
        return consumer.on_finished
