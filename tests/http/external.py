import unittest
import asyncio

from pulsar import get_actor

from .base import TestHttpClientBase


class ExternalBase(TestHttpClientBase):
    with_httpbin = False

    def after_response(self, response):
        pass

    @asyncio.coroutine
    def test_get_https(self):
        client = self.client()
        response = yield from client.get('https://github.com/trending')
        self.assertEqual(response.status_code, 200)


class ProxyExternal(ExternalBase):

    def after_response(self, response):
        self.assertTrue(response.request.proxy)

    @asyncio.coroutine
    def test_get_https(self):
        client = self.client()
        response = yield from client.get('https://github.com/trending')
        self.assertEqual(response.status_code, 200)


@unittest.skipUnless(get_actor().cfg.http_proxy == '',
                     'Requires no external proxy')
class Test_HttpClient_NoProxy_External(ExternalBase, unittest.TestCase):
    '''Test external URI when no global proxy server is present.
    '''


@unittest.skipUnless(get_actor().cfg.http_proxy == '',
                     'Requires no external proxy')
class Test_HttpClient_Proxy_External(ProxyExternal, unittest.TestCase):
    with_proxy = True


@unittest.skipUnless(get_actor().cfg.http_proxy, 'Requires external proxy')
class Test_HttpClient_ExternalProxy_External(ProxyExternal,
                                             unittest.TestCase):
    pass
