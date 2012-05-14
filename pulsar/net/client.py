'''Asynchronous HTTP client'''
import socket

from pulsar.utils import httpurl
from pulsar import AsyncIOStream
from pulsar.utils.sock import create_socket

HttpClient = httpurl.HttpClient

__all__ = ['HttpClient']


class http_async_handler(httpurl.HTTPAsyncConnection):
    
    def connect(self):
        sock = create_socket((self.host,self.port))
        if self.source_address:
            sock.bind(self.source_address)
        self.sock = AsyncIOStream(sock)
        self.sock.connect()
    

httpurl.set_async_connection('http', http_async_handler)
httpurl.set_async_connection('http', http_async_handler)