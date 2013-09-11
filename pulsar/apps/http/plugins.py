from functools import partial

from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.websocket import FrameParser
from pulsar.async.stream import SocketStreamSslTransport
from pulsar.utils.httpurl import REDIRECT_CODES, urlparse, urljoin

from pulsar import get_actor


class TooManyRedirects(Exception):
    pass


class HandleRedirectAndCookies:
    
    def __call__(self, response):
        request = response._request
        headers = response.headers
        client = request.client
        # store cookies in client if needed
        if client.store_cookies and 'set-cookie' in headers:
            client.cookies.extract_cookies(response, request)
        # check redirect
        if (response.status_code in REDIRECT_CODES and
            'location' in headers and
            request.allow_redirects and
            request.parser.is_message_complete()):
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
            if len(response.history) >= request.max_redirects:
                raise TooManyRedirects
            url = url or request.full_url
            #
            new_response = client.upgrade(response.connection,
                                          release_connection=True)
            new_response.history.extend(response.history)
            new_response.append(response)
            #
            rparams = request.all_params()
            method = request.method
            if response.status_code == 303:
                method = 'GET'
                rparams.pop('data')
                rparams.pop('files')
            rparams['response'] = new_response
            client.request(request.method, url, rparams)


class Handle100:
    
    def __call__(self, response):
        if response.status_code == 100:
            # reset the parser
            request = response._request
            request.new_parser()
            response.transport.write(request.body)
            

class Handle101:

    def __call__(self, response):
        if response.status_code == 101:
            client = response.producer
            request = response.current_request
            handler = request.websocket_handler
            parser = FrameParser(kind=1)
            if not handler:
                handler = WS()
            factory = partial(WebSocketProtocol, response, handler, parser)
            client.upgrade(response.connection, factory)


class TunnelRequest:
    __slots__ = ['request']
    
    def __init__(self, request):
        self.request = request
        
    def encode(self):
        first_line = 'CONNECT %s HTTP/1.0\r\n' % self.request._netloc
        headers = bytes(self.request.tunnel_headers)
        return b''.join((first_line.encode('ascii'), headers))
    
        
class Tunneling:
    '''A callback for handling proxy tunneling.
    
    The callable method is added as ``pre_request`` to a :class:`HttpClient`.
    If Tunneling is required, it writes the CONNECT headers and abort
    the writing of the actual request until headers from the proxy server
    are received.
    '''
    def __call__(self, response):
        # Called before sending the request
        request = response._request
        if request._proxy and request._ssl:
            response._request = TunnelRequest(request)
            response.bind_event('on_message_complete', self.on_message_complete)
            
    def on_message_complete(self, response):
        '''Called back once the message is complete.'''
        if response.status_code == 200:
            # Copy request so that the HttpResponse does not conclude
            request = response._request.request
            request.set_proxy(None)
            loop = response.event_loop
            new_response = response.producer.upgrade(response.connection)
            loop.remove_reader(new_response.transport.sock.fileno())
            # Wraps the socket at the next iteration loop. Important!
            loop.call_soon(self.switch_to_ssl, request, new_response)
            
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
        