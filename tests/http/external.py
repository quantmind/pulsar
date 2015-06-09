import socket
import unittest

from pulsar import get_actor

from .base import TestHttpClientBase


class ExternalBase(TestHttpClientBase):
    with_httpbin = False

    def after_response(self, response):
        pass

    def ___test_http_get_timeit(self):
        client = self.client()
        N = 20
        responses = yield from client.timeit(N, 'get', 'http://www.bbc.co.uk/')
        self.assertEqual(len(responses), N)
        for n in range(N):
            all = []

            def save_data(r, data=None):
                all.append(data)
            try:
                response = yield from client.get('http://www.theguardian.com/',
                                                 data_received=save_data)
            except Exception:
                for n, d in enumerate(all):
                    with open('data%s.dat' % n, 'wb') as f:
                        f.write(d)
                raise
            self.assertEqual(response.status_code, 200)

    def __test_http_get(self):
        client = self.client()
        response = yield from client.get('http://www.bbc.co.uk/')
        self.assertEqual(response.status_code, 200)
        self.after_response(response)

    def test_get_https(self):
        client = self.client()
        response = yield from client.get('https://github.com/trending')
        self.assertEqual(response.status_code, 200)

    def __test_bad_host(self):
        client = self.client()
        try:
            response = yield from client.get('http://xxxyyyxxxxyyy/blafoo')
        except socket.error:
            pass
        else:
            self.assertTrue(response.request.proxy)
            self.assertTrue(response.status_code >= 400)


class ProxyExternal(ExternalBase):

    def after_response(self, response):
        self.assertTrue(response.request.proxy)

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
