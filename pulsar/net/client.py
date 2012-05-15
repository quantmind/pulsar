'''Asynchronous HTTP client'''
import socket

from pulsar.utils import httpurl
from pulsar import AsyncIOStream
from pulsar.utils.sock import create_socket

from . import base

HttpClient = httpurl.HttpClient

__all__ = ['HttpClient']


class HttpAsyncConnection(base.NetStream):
    default_port = 80
    def __init__(self, host, port=None, source_address=None):
        self.host = host
        self.port = port or self.default_port
        self.source_address = source_address
        sock = create_socket((self.host,self.port), bound=True)
        super(HttpAsyncConnection, self).__init__(AsyncIOStream(sock))
        
    def connect(self):
        self.stream.connect((self.host,self.port), callback=self.on_connect)
        return self
    
    def request(method, path, body, headers):
        pass
    
    def on_connect(self, result):
        if self.source_address:
            sock.bind(self.source_address)
            
    

httpurl.set_async_connection('http', HttpAsyncConnection)
httpurl.set_async_connection('http', HttpAsyncConnection)