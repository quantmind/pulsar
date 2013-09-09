from . import client

class TestTlsHttpClientWithProxy(client.TestHttpClient):
    with_proxy = True
    with_tls = True