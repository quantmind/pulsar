'''Asynchronous HTTP client'''
import socket

import pulsar
from pulsar import lib
from pulsar.utils import httpurl
from pulsar.utils.httpurl import Request, HTTPResponseMixin
from pulsar.net.base import NetRequest, AsyncClientResponse


__all__ = ['HttpClient']


    
class AsyncHttpResponse(AsyncClientResponse, HTTPResponseMixin):

    def done(self):
        return self.parser.is_message_complete() 
    
    def start(self, req, headers):
        self.request.request(req.get_method(), req.selector, req.data, headers)
        return self.read()
        
    def post_process_response(self, client, req):
        return self._event.add_callback(
                        lambda r: client.post_process_response(req, r))
    
    def finish(self):
        self._content = self.parser.recv_body()
    
    @property
    def headers(self):
        return self.parser._headers
    
    @property
    def code(self):
        return self.parser._status_code
    
    @property
    def version(self):
        return self.parser._versions
    
    # For compatibility with old-style urllib responses.

    def info(self):
        return self.headers

    def geturl(self):
        return self.url

    def getcode(self):
        return self.status
    
    
class AsyncRequest(Request):
    
    def get_response(self, headers):
        c = self.connection
        response = AsyncHttpResponse(c)
        response.protocol = self.type
        response.url = self.full_url
        c.connect().add_callback(lambda r: response.start(self, headers),
                                 response.error)
        return response

    
class HttpAsyncConnection(NetRequest):
    
    def default_parser(self):
        return lib.Http_Parser
    
    def get_parser(self, parsercls, **kwargs):
        return parsercls(kind='server')
    
    def connect(self): 
        return super(HttpAsyncConnection, self).connect((self.host,self.port))
        
    def on_connect(self, result):
        if self.source_address:
            self.stream.socket.bind(self.source_address)
            
    
class HTTPConnection(HttpAsyncConnection, httpurl.HTTPConnection):
    
    def __init__(self, *args, **kwargs):
        httpurl.HTTPConnection.__init__(self, *args, **kwargs)
        super(HTTPConnection, self).__init__()
        

class HttpClient(httpurl.HttpClient):
    client_version = pulsar.SERVER_SOFTWARE
    request_class = AsyncRequest
    Connections = {'http': HTTPConnection}