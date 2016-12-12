from pulsar.apps.test import skipUnless
from pulsar.utils.system import platform

from tests.http import base, req


@skipUnless(platform.type != 'win', 'Hangs on appveyor - need fixing')
class TestTlsHttpClientWithProxy(req.TestRequest, base.TestHttpClient):
    with_proxy = True
    with_tls = True
