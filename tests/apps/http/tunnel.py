from pulsar.utils.httpurl import urlparse

from . import client


#class TestHttpClient(client.TestHttpClientBase, client.unittest.TestCase):

class TestTlsHttpClientWithProxy(client.TestHttpClient):
    with_proxy = True
    with_tls = True

    def after_test_home_page(self, response):
        request = response.request
        self.assertEqual(request.scheme, 'https')
        self.assertEqual(request.proxy, None)
        # Only one connection pool,
        # even though the proxy and the connection are for different addresses
        http = response.producer
        self.assertEqual(len(http.connection_pools), 1)
        pool = http.connection_pools[response.request.key]
        self.assertEqual(pool.received, 1)

    def __test_tunnel_request_object(self):
        http = self.client()
        response = yield http.get(self.httpbin()).on_finished
        request = response.request
        tunnel = request._tunnel
        self.assertEqual(tunnel.request, request)
        self.assertNotEqual(tunnel.parser, request.parser)
        self.assertEqual(tunnel.full_url, self.proxy_uri)
        self.assertEqual(tunnel.headers['host'], urlparse(self.uri).netloc)
