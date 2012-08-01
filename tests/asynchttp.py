'''tests the httpurl stand-alone script.'''
import pulsar
from pulsar import is_async

from . import httpurl


class TestHttpClient(httpurl.TestHttpClient):
    HttpClient = pulsar.HttpClient
    
    def make_async(self, r):
        self.assertTrue(is_async(r))
        return r