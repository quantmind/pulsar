'''Benchmark the HttpClient in pulsar.net.client.'''
from pulsar.net import HttpClient, HttpClients
from pulsar.utils.test import test

HTTPBIN_URL = 'http://httpbin.ep.io/'
HTTPSBIN_URL = 'https://httpbin.ep.io/'

def httpbin(*suffix):
    """Returns url for HTTPBIN resource."""
    return HTTPBIN_URL + '/'.join(suffix)


def httpsbin(*suffix):
    """Returns url for HTTPSBIN resource."""
    return HTTPSBIN_URL + '/'.join(suffix)


class TestStandardHttpClient(test.TestCase):
    client = 1
    
    def setUp(self):
        proxy = self.worker.cfg.http_proxy
        proxy_info = {}
        if proxy:
            proxy_info['http'] = proxy
        self.r = HttpClient(type = self.client, proxy_info = proxy_info)
        self.url = httpbin()
        
    def test_http_200_get(self):
        r = self.r.get(self.url)
        

@test.skipUnless(2 in HttpClients,'httplib2 not installed.')
class TestHttplib2Client(TestStandardHttpClient):
    client = 2