from base64 import b64encode, b64decode

import pulsar
from pulsar.apps.ws import WebSocketProtocol, WS
from pulsar.utils.pep import native_str
from pulsar.utils.websocket import FrameParser


__all__ = ['WebSocketResponse']
    
    
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
        
