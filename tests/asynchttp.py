'''Tests asynchronous HttpClient.'''
from . import httpurl

class TestHttpClient(httpurl.TestHttpClient):
    timeout = 0