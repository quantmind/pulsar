from tests.http import base, req


class TestTlsHttpClientWithProxy(base.TestHttpClient, req.TestRequest):
    with_proxy = True
    with_tls = True
