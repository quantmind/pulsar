from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.websocket import FrameParser
from pulsar.async.stream import SocketStreamSslTransport

from pulsar import get_actor

    
class WebSocketResponse(WebSocketProtocol):
    
    def __init__(self, handshake, connection, parser=None):
        # keep a reference to the websocket
        super(WebSocketResponse, self).__init__(connection)
        self.handshake = handshake
        connection.set_timeout(0)
        self.parser = parser or FrameParser(kind=1)
        self.handler = handshake.current_request.websocket_handler
        if not self.handler:
            self.handler = WS()


class Tunneling:
    '''A callback for handling proxy tunneling.
    
    The callable method is added as ``pre_request`` to a :class:`HttpClient`.
    If Tunneling is required, it writes the CONNECT headers and abort
    the writing of the actual request until headers from the proxy server
    are received.
    '''
    def __call__(self, response):
        # Called before sending the request
        request = response.current_request
        if request and request._proxy and request._ssl:
            first_line = 'CONNECT %s HTTP/1.0\r\n' % request._netloc
            headers = bytes(request.tunnel_headers)
            response._data_sent = b''.join((first_line.encode('ascii'),
                                            headers))
            response.bind_event('on_headers', self.on_headers)
            response.transport.write(response._data_sent)
            
    def on_headers(self, response):
        '''Called back once the headers are ready.'''
        if (response.status_code == 200 and
            response._data_sent[:7] == b'CONNECT'):
            # Copy request so that the HttpResponse does not conclude
            response._current_request = response._current_request.copy()
            # Remove the proxy from the request
            response._current_request.set_proxy(None)
            loop = response.event_loop
            loop.remove_reader(response.transport.sock.fileno())
            # Wraps the socket at the next iteration loop. Important.
            loop.call_soon(self.switch_to_ssl, response)
            
    def switch_to_ssl(self, response):
        '''Wrap the transport for SSL communication.'''
        request = response.current_request
        loop = response.event_loop
        transport = response.transport
        sock = transport.sock
        transport = SocketStreamSslTransport(loop, sock, transport.protocol,
                                             request._ssl, server_side=False,
                                             server_hostname=request._netloc)
        response._data_sent = request.encode()
        transport.write(response._data_sent)
        
