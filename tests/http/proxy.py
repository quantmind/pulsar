from . import client

class TestHttpClientWithProxy(client.TestHttpClient):
    with_proxy = True