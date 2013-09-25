import socket

from pulsar import get_actor
from pulsar.apps import http
from pulsar.apps.test import unittest
from pulsar.apps.http import URLError

from .client import TestHttpClientBase


@unittest.skipUnless(get_actor().cfg.http_proxy=='', 'No proxy')
class TestHttpClientBaseNoProxy(TestHttpClientBase, unittest.TestCase):
    '''Test external URI when no global proxy server is present.
    '''
    with_httpbin = False
    concurrency = 'thread'

    def test_http_get(self):
        client = self.client()
        response = yield client.get('http://www.amazon.co.uk/').on_finished
        self.assertEqual(response.status_code, 200)

    def test_https_get(self):
        client = self.client()
        response = yield client.get(
            'https://api.github.com/users/lsbardel/repos').on_finished
        self.assertEqual(response.status_code, 200)

    def test_bad_host(self):
        client = self.client()
        response = client.get('http://xxxyyyxxxxyyy/blafoo')
        try:
            yield response.on_finished
        except socket.error:
            pass
        self.assertFalse(response.status_code)
        self.assertTrue(response.is_error)
        self.assertRaises(URLError, response.raise_for_status)


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


@unittest.skipUnless(get_actor().cfg.http_proxy, 'Requires external proxy')
class TestHttpClientExternalProxy(TestHttpClientBase, unittest.TestCase):
    with_httpbin = False

    def test_get_httpbin(self):
        client = self.client()
        response = yield client.get('http://httpbin.org/').on_finished
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.request.proxy)
