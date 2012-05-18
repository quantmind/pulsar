'''Asynchronous HTTP client'''
import socket

from pulsar import Deferred, lib
from pulsar.utils import httpurl
from pulsar.net import base

HttpClient = httpurl.HttpClient

__all__ = ['HttpClient']


class HttpClientResponse(base.ClientResponse, httpurl.HTTPResponse):
    
    def start_request(self, result):
        try:
            self.request._send_request(method, path, body, headers)
        except Exception as e:
            self.callback(e)
                
    def post_process_response(self, client, req):
        call = super(HttpClientResponse, self).post_process_response
        self.add_callback(lambda r: call(client, req))
        return self

    
class HttpAsyncConnection(base.ClientConnection):
    response_class = HttpClientResponse
    
    def connect(self): 
        return super(HttpAsyncConnection, self).connect((self.host,self.port))
    
    def request(self, method, path, body, headers):
        # This would look much better as a generator, but we
        # are using the same class a synchronous connection
        self.response = r = HttpClientResponse(self)
        return self.connect().add_callback(r.start_request, r.callback)
    
    def getresponse(self):
        '''This call should be after :meth:`request'''
        return self.__dict__.pop('response')
    
    def callback_write(self, bytes_sent):
        # We are gioing to start reading
        
        
    def on_connect(self, result):
        if self.source_address:
            self.stream.socket.bind(self.source_address)
            
    
class HTTPConnection(HttpAsyncConnection, httpurl.HTTPConnection):
    
    def __init__(self, *args, **kwargs):
        httpurl.HTTPConnection.__init__(self, *args, **kwargs)
        super(HTTPConnection, self).__init__()

httpurl.set_async_connection('http', HTTPConnection)
#httpurl.set_async_connection('http', HTTPsConnection)
