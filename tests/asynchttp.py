'''tests the httpurl stand-alone script.'''
from pulsar import is_async, net

from . import httpurl


class TestHttpClient(httpurl.TestHttpClient):
    HttpClient = net.HttpClient
    
    def make_async(self, r):
        self.assertTrue(is_async(r))
        return r