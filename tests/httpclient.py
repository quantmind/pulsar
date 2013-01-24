'''Tests asynchronous HttpClient.'''
from pulsar import make_async

from . import httpurl

class TestAsyncHttpClient(httpurl.TestHttpClient):
    timeout = 0        
        
        
#class TestAsyncHttpClientWithProxy(TestAsyncHttpClient):
#    with_proxy = True