from pulsar import Http404
from pulsar.apps.wsgi import route

from .websocket import WebSocket, register_transport, TRANSPORTS


__all__ = ['SoketIO']


class SocketIO(WebSocket):
    
    def __init__(self, path='socket.io', handler=None, transports=None,
                 heartbeat_timeout=None, close_timeout=None):
        super(SocketIO, self).__init__(path, handler)
        self.transports = []
        self.heartbeat_timeout = heartbeat_timeout or ''
        self.close_timeout = close_timeout or ''
        transports = transports or list(TRANSPORTS)
        for transport in transports:
            Middleware = TRANSPORTS.get(transport)
            if not Middleware:
                continue
            self.transports.append(transport)
            router = Middleware('/<version>/%s/<session>' % transport, handler)
            self.add_child(router)
    
    def get(self, request):
        raise Http404
    
    @route('/<version>/', method='get')
    def handshake(self, request):
        url_data = request.url_data
        response = request.response
        data = '%s:%s:%s:%s' % (self.session_id(request),
                                self.heartbeat_timeout,
                                self.close_timeout,
                                ','.join(self.transports))
        response.content_type = 'text/plain'
        response.content = data
        return response
    
    def session_id(self, request):
        return request.environ['pulsar.connection'].session
        

class XhrParser(object):
    pass
    
    
@register_transport
class XhrPolling(WebSocket):
    _name = 'xhr-polling'
    protocol_parser = XhrParser
    