from pulsar import get_actor, send
from pulsar.apps import http
from pulsar.apps.test import unittest

from .client import TestHttpClientBase

@unittest.skipUnless(get_actor().cfg.http_proxy=='', 'No proxy')
class TestHttpClientBaseNoProxy(TestHttpClientBase, unittest.TestCase):
    with_httpbin = False
    concurrency = 'thread'

    def test_http_get(self):
        client = self.client()
        response = yield client.get('http://www.amazon.co.uk/').on_finished
        self.assertEqual(response.status_code, 200)
        
    def __test_https_get(self):
        client = self.client()
        response = yield client.get(
            'https://api.github.com/users/lsbardel/repos').on_finished
        self.assertEqual(response.status_code, 200)
        

class d:
#class TestTunnelExternal(TestHttpClientBaseNoProxy):
    with_proxy = True
    
    def test_get(self):
        client = self.client()
        response = client.get('https://github.com/trending')
        r1 = yield response.on_headers
        self.assertEqual(r1.status_code, 200)
        r2 = yield response.on_finished
        self.assertEqual(r2.status_code, 200)