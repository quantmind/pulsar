import unittest

from pulsar import get_actor
from pulsar.utils.system import platform

from tests.http import base, req


if platform.type != 'win':

    @unittest.skipIf(get_actor().cfg.event_loop == 'uv',
                     "uvloop does not work with udp servers")
    class TestTlsHttpClientWithProxy(req.TestRequest, base.TestHttpClient):
        with_proxy = True
        with_tls = True
