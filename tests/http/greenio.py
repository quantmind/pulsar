import unittest

from pulsar.apps.http import HttpClient

try:
    from pulsar.apps import greenio
    run_in_greenlet = greenio.run_in_greenlet
except ImportError:
    greenio = None
    run_in_greenlet = lambda x: x

from . import base


@unittest.skipUnless(greenio, 'Requires the greenlet package')
class TestHttpClientGreenlet(base.TestHttpClientBase, unittest.TestCase):

    @classmethod
    def client(cls, pool_size=2, green=True, **kwargs):
        return HttpClient(pool_size=pool_size, green=True, **kwargs)

    def test_client(self):
        http = self._client
        self.assertTrue(http.green)

    def test_not_in_greenlet(self):
        http = self._client
        self.assertRaises(AssertionError, http.get, self.httpbin('get'))

    @run_in_greenlet
    def test_get(self):
        http = self._client
        response = http.get(self.httpbin('get'))
        self.assertEqual(response.status_code, 200)
