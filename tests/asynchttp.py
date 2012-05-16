'''tests the httpurl stand-alone script.'''
from pulsar import is_async
from pulsar.utils.test import test
from pulsar.net import HttpClient

from .httpurl import httpbin


class TestHttpClient(test.TestCase):
    
    def setUp(self):
        proxy = self.worker.cfg.http_proxy
        proxy_info = {}
        if proxy:
            proxy_info['http'] = proxy
        self.r = HttpClient(proxy_info=proxy_info,timeout=0)
         
    def test_http_200_get(self):
        r = self.r.get(httpbin())
        self.assertTrue(is_async(r))
        yield r
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        self.assertTrue(r.content)
        self.assertEqual(r.url, httpbin())