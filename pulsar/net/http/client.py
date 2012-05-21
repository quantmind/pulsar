'''Asynchronous HTTP client'''
import socket

from pulsar import Deferred, lib
from pulsar.utils import httpurl
from pulsar.net import base

HttpClient = httpurl.HttpClient

__all__ = ['HttpClient']

class Request(base.NetRequest, httpurl.Request):

    def default_parser(self):
        return lib.Http_Parser
        
    
class HttpClient(httpurl.HttpClient):
    request_class = Request


class HttpClientResponse(base.NetResponse, httpurl.HTTPResponse, Deferred):
    
    def start_request(self, result):
        req = self.request
        try:
            request._send_request(method, path, body, headers)
            self.begin()
        except Exception as e:
            self.callback(e)
    
    def begin(self, data=None):
        if data is None:
            self.stream.read().add_callback(self.begin)
        
    def post_process_response(self, client, req):
        call = httpurl.HTTPResponse.post_process_response
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
        pass
        
    def on_connect(self, result):
        if self.source_address:
            self.stream.socket.bind(self.source_address)
            
    
class HTTPConnection(HttpAsyncConnection, httpurl.HTTPConnection):
    
    def __init__(self, *args, **kwargs):
        httpurl.HTTPConnection.__init__(self, *args, **kwargs)
        super(HTTPConnection, self).__init__()

httpurl.set_async_connection('http', HTTPConnection)
#httpurl.set_async_connection('http', HTTPsConnection)
