'''Asynchronous HTTP client'''
import socket

import pulsar
from pulsar import lib
from pulsar.utils import httpurl


__all__ = ['HttpClient']
    
    
class AsyncRequest(httpurl.HttpRequest):
    
    def get_response(self, history=None):
        response = super(AsyncRequest, self).get_response(history=history)
        if not response.history:
            dr = DeferredResponse(response)
            response.on_finished = dr
            return dr
        else:
            return response
        
    def on_response(self, response):
        return response
        

class HttpClient(httpurl.HttpClient):
    client_version = pulsar.SERVER_SOFTWARE
    request_class = AsyncRequest
    