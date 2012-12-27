'''Asynchronous HTTP client'''
import pulsar
from pulsar import lib
from pulsar.utils import httpurl

from .iostream import AsyncIOStream


__all__ = ['HttpClient']
    

class HttpConnection(httpurl.HttpConnection):
    
    def connect(self):
        if self.timeout == 0:
            self.sock = AsyncIOStream()
            self.sock.connect((self.host, self.port))
            if self._tunnel_host:
                self._tunnel()
        else:
            httpurl.HttpConnection.connect(self)
            
    @property
    def closed(self):
        if self.timeout == 0:
            if not self.sock.closed:
                return httpurl.is_closed(self.sock.sock)
            else:
                return True
        else:
            return httpurl.is_closed(self.sock)
        

class HttpClient(httpurl.HttpClient):
    timeout = 0
    client_version = pulsar.SERVER_SOFTWARE
    http_connection = HttpConnection
    