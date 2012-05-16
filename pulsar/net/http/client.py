'''Asynchronous HTTP client'''
import socket

from pulsar import Deferred, lib
from pulsar.utils import httpurl
from pulsar.net import base

HttpClient = httpurl.HttpClient

__all__ = ['HttpClient']


class HttpClientResponse(base.ClientResponse, httpurl.HTTPResponse):
    
    def post_process_response(self, client, req):
        call = super(HttpClientResponse, self).post_process_response
        self.add_callback(lambda r: call(client, req))
        return self

    
class HttpAsyncConnection(base.ClientConnection):
    response_class = HttpClientResponse
    
    def __init__(self, host, port=None, source_address=None):
        self.host = host
        self.port = port or self.default_port
        self.source_address = source_address
        super(HttpAsyncConnection, self).__init__()
        
    def connect(self): 
        return super(HttpAsyncConnection, self).connect((self.host,self.port))
    
    def request(self, method, path, body, headers, first=True):
        if first:
            return self.connect().add_callback(
                lambda r: self.request(method, path, body, headers, r))
    
    def getresponse(self):
        '''This call should be after :meth:`request'''
        return HttpClientResponse(self)
    
    def on_connect(self, result):
        if self.source_address:
            self.stream.socket.bind(self.source_address)
            
    
class HTTPConnection(HttpAsyncConnection, httpurl.HTTPConnection):
    pass

httpurl.set_async_connection('http', HTTPConnection)
#httpurl.set_async_connection('http', HTTPsConnection)
