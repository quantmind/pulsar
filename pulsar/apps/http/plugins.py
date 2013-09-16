from functools import partial

from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.websocket import FrameParser
from pulsar.async.stream import SocketStreamSslTransport
from pulsar.utils.httpurl import REDIRECT_CODES, urlparse, urljoin, requote_uri

from pulsar import get_actor, PulsarException


class TooManyRedirects(PulsarException):
    
    def __init__(self, response):
        self.response = response


def handle_redirect(response, _):
    if (response.status_code in REDIRECT_CODES and
        'location' in response.headers and
        response._request.allow_redirects):
        # put at the end of the pile
        response.bind_event('post_request', _do_redirect)
    return response
        
def _do_redirect(response, _):
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

def handle_cookies(response, _):
    headers = response.headers
    request = response.request
    client = request.client
    if client.store_cookies and 'set-cookie' in headers:
        client.cookies.extract_cookies(response, request)
    return headers
    
def handle_100(response, _):
    if response.status_code == 100:
        # reset the parser
        request = response._request
        request.new_parser()
        response.transport.write(request.body)
    return response

def handle_101(response, _):
    '''Websocket upgrade as ``on_headers`` event.'''
    if response.status_code == 101:
        connection = response.connection
        request = response._request
        handler = request.websocket_handler
        parser = FrameParser(kind=1)
        if not handler:
            handler = WS()
        factory = partial(WebSocketProtocol, response, handler, parser)
        response = connection.upgrade(factory, True)
    return response


class TunnelRequest:
    __slots__ = ['request']
    
    def __init__(self, request):
        self.request = request
        self.parser = request.parser
        request.new_parser()
        
    def encode(self):
        first_line = 'CONNECT %s HTTP/1.1\r\n' % self.request._netloc
        headers = bytes(self.request.tunnel_headers)
        return b''.join((first_line.encode('ascii'), headers))
    
        
class Tunneling:
    '''A callback for handling proxy tunneling.
    
    The callable method is added as ``pre_request`` to a :class:`HttpClient`.
    If Tunneling is required, it writes the CONNECT headers and abort
    the writing of the actual request until headers from the proxy server
    are received.
    '''
    def __call__(self, response, request):
        # the pre_request handler
        if request._proxy and request._ssl:
            response._request = TunnelRequest(request)
            response.bind_event('post_request', self.post_request)
            
    def post_request(self, response, result):
        '''Called back once the message is complete.'''
        if response.status_code == 200:
            request = response._request.request
            request.set_proxy(None)
            old_response = response
            loop = response.event_loop
            response = old_response.producer.upgrade(old_response.connection)
            loop.remove_reader(response.transport.sock.fileno())
            # Wraps the socket at the next iteration loop. Important!
            loop.call_soon(self.switch_to_ssl, request, response)
        return response
        
    def switch_to_ssl(self, request, response):
        '''Wrap the transport for SSL communication.'''
        loop = response.event_loop
        transport = response.transport
        sock = transport.sock
        transport = SocketStreamSslTransport(loop, sock, transport.protocol,
                                             request._ssl, server_side=False,
                                             server_hostname=request._netloc)
        response._connection._transport = transport
        response.start(request)
        