from pulsar.apps import http
from pulsar.apps.test import unittest



class TestTunnel(unittest.TestCase):
    
    def test_get(self):
        client = http.HttpClient()
        response = client.get('https://github.com/trending')
        r1 = yield response.on_headers
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r1.status, '200 Connection established')
        r2 = yield response.on_finished
        self.assertEqual(r2.status_code, 200)