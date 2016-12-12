from pulsar.utils.system import platform

from tests.http import base, req


if platform.type != 'win':

    class TestTlsHttpClientWithProxy(req.TestRequest, base.TestHttpClient):
        with_proxy = True
        with_tls = True
