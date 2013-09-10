from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.websocket import FrameParser

from pulsar import get_actor

    
class WebSocketResponse(WebSocketProtocol):
    
    def __init__(self, connection, handshake, parser=None):
        # keep a reference to the websocket
        self.handshake = handshake
        handshake.finished(self)
        super(WebSocketResponse, self).__init__(connection)
        connection.set_timeout(0)
        self.parser = parser or FrameParser(kind=1)
        self.handler = handshake.current_request.websocket_handler
        if not self.handler:
            self.handler = WS()


class Tunneling:
    
    def __call__(self, response):
        # Called before sending the request
        request = response.current_request
        if request._scheme in ('https', 'wss') and not request.ssl:
            first_line = 'CONNECT %s HTTP/1.0\r\n' % request._netloc
            headers = bytes(request.tunnel_headers)
            response._data_sent = b''.join((first_line.encode('ascii'),
                                            headers))
            response.bind_event('on_headers', self.on_headers)
            response.transport.write(response._data_sent)
            
    def on_headers(self, response):
        '''Called back once the headers are ready.'''
        request = response.current_request
        if response.status_code == 200 and response._data_sent[:7] == 'CONNECT':
            response._data_sent = request.encode()
            response.transport.write(response._data_sent)
        
