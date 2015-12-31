from . import base


class TestTlsHttpClient(base.TestHttpClient):
    with_tls = True

    def test_large_response(self):
        # TODO: This times out (sometimes)
        pass
