import socket

import pulsar
from pulsar import async_func_call

__all__ = ['NetStream','NetRequest','NetResponse','close_socket']


def close_socket(sock):
    if sock:
        try:
            sock.close()
        except socket.error:
            pass


class NetStream(object):
    
    def __init__(self, stream, **kwargs):
        self.stream = stream
        self.on_init(kwargs)
    
    @property
    def actor(self):
        return self.stream.actor
            
    def close(self):
        yield self.on_close()
        yield self.stream.close()
            
    def on_init(self, kwargs):
        pass
    
    def on_close(self):
        pass


class NetRequest(NetStream):
    '''A HTTP parser providing higher-level access to a readable,
sequential io.RawIOBase object. You can use implementions of
http_parser.reader (IterReader, StringReader, SocketReader) or 
create your own.'''
    default_parser = None
    
    def __init__(self, stream, client_addr = None, parsercls = None, **kwargs):
        self.parsercls = parsercls or self.default_parser
        self.client_address = client_addr
        self.parser = self.get_parser(**kwargs)
        super(NetRequest,self).__init__(stream, **kwargs)
        
    def get_parser(self, **kwargs):
        if self.parsercls:
            return self.parsercls()
        
    
class NetResponse(NetStream,pulsar.Response):
    '''A HTTP parser providing higher-level access to a readable,
sequential io.RawIOBase object. You can use implementions of
http_parser.reader (IterReader, StringReader, SocketReader) or 
create your own.'''
    def __init__(self, request, stream = None, **kwargs):
        pulsar.Response.__init__(self,request)
        stream = stream or self.request.stream
        self.version = pulsar.SERVER_SOFTWARE
        NetStream.__init__(self, stream, **kwargs)
    
    