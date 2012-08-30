'''Asynchronous HTTP client'''
import pulsar
from pulsar import lib
from pulsar.utils import httpurl

from .iostream import AsyncIOStream


__all__ = ['HttpClient']
    

class HttpConnectionPool(httpurl.HttpConnectionPool):
    
    def make_connection(self):
        conn = super(HttpConnectionPool, self).make_connection()
        if conn.timeout == 0:
            conn.sock = AsyncIOStream()
            conn.sock.connect((conn.host, conn.port))
        return conn
    

class HttpResponse(httpurl.HttpResponse):
    pass
    
    
class AsyncRequest(httpurl.HttpRequest):
    response_class = HttpResponse
    def on_response(self, response):
        return response
        

class HttpClient(httpurl.HttpClient):
    timeout = 0
    client_version = pulsar.SERVER_SOFTWARE
    connection_pool = HttpConnectionPool
    request_class = AsyncRequest
    