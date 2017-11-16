from tests.http import base, req


class TestHttpClientWithProxy(req.TestRequest, base.TestHttpClient):
    with_proxy = True

    def _check_server(self, response):
        pass
