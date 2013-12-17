from pulsar.utils.httpurl import urlparse

from . import base


#class TestHttpClient(client.TestHttpClientBase, client.unittest.TestCase):

class TestTlsHttpClientWithProxy(base.TestHttpClient):
    with_proxy = True
    with_tls = True
