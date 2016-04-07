from tests.http import base, req


class TestHttpClientWithProxy(base.TestHttpClient, req.TestRequest):
    with_proxy = True

    def _check_server(self, response):
        pass
