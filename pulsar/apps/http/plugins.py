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
        history = request.history
        if request._requires_tunneling and history:
            if history[-1].current_request._requires_tunneling:
                history.pop()
                request._requires_tunneling = False
        if request._requires_tunneling:
            callback = getattr(response, '_tunneling_callback', None)
            if not callback:
                response._tunneling_callback = response.bind_event(
                    'on_headers', self.on_headers)
            
    def on_headers(self, response):
        '''Called back once the headers are ready.'''
        request = response.current_request
        if request._requires_tunneling and response.status_code == 200:
            print(request.full_url)
            print(request.history)
            request.client.request(request.method, request.full_url,
                                   response=response)
        
