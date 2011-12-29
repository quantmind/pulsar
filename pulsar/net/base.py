import socket

import pulsar

__all__ = ['NetStream','NetRequest','NetResponse','close_socket']


def close_socket(sock):
    if sock:
        try:
            sock.close()
        except socket.error:
            pass


class NetStream(object):
    
    def __init__(self, stream, timeout = None, **kwargs):
        self.stream = stream
        self.timeout = timeout
        self.on_init(kwargs)
    
    def fileno(self):
        return self.stream.fileno()
    
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
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self.fileno())
    
    def __str__(self):
        return self.__repr__()


class NetRequest(NetStream):
    '''A HTTP parser providing higher-level access to a readable,
sequential io.RawIOBase object. You can use implementions of
http_parser.reader (IterReader, StringReader, SocketReader) or 
create your own.'''    
    def __init__(self, stream, client_addr = None, parsercls = None, **kwargs):
        self.parsercls = parsercls or self.default_parser()
        self.client_address = client_addr
        self.parser = self.get_parser(**kwargs)
        super(NetRequest,self).__init__(stream, **kwargs)
        
    def default_parser(self):
        return None
        
    def get_parser(self, **kwargs):
        if self.parsercls:
            return self.parsercls()
    
        
class NetResponse(NetStream, pulsar.Response):
    '''A HTTP parser providing higher-level access to a readable,
sequential io.RawIOBase object. You can use implementions of
http_parser.reader (IterReader, StringReader, SocketReader) or 
create your own.'''
    def __init__(self, request, stream = None, **kwargs):
        pulsar.Response.__init__(self,request)
        stream = stream or self.request.stream
        timeout = kwargs.pop('timeout',request.timeout)
        self.version = pulsar.SERVER_SOFTWARE
        NetStream.__init__(self, stream, timeout = timeout, **kwargs)
    
    