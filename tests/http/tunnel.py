from . import base


class TestTlsHttpClientWithProxy(base.TestHttpClient):
    with_proxy = True
    with_tls = True
