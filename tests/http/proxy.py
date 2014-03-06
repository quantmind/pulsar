from . import base


class TestHttpClientWithProxy(base.TestHttpClient):
    with_proxy = True

    def _check_server(self, response):
        pass
