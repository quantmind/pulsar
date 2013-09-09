from . import client

class TestTlsHttpClient(client.TestHttpClient):
    with_tls = True