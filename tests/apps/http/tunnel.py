from . import client

class TestTlsHttpClientWithProxy(client.TestHttpClient):
    with_proxy = True
    with_tls = True
    _created_connections = 2
    
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