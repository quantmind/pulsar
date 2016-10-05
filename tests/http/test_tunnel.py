from tests.http import base, req


class TestTlsHttpClientWithProxy(req.TestRequest, base.TestHttpClient):
    with_proxy = True
    with_tls = True
