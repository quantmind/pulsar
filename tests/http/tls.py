import os
import asyncio

from pulsar.apps.http import SSLError, HttpClient
from . import base


crt = os.path.join(os.path.dirname(__file__), 'ca_bundle')


class TestTlsHttpClient(base.TestHttpClient):
    with_tls = True

    @asyncio.coroutine
    def test_verify(self):
        c = HttpClient()
        yield from self.async.assertRaises(SSLError, c.get, self.httpbin())
        response = yield from c.get(self.httpbin(), verify=False)
        self.assertEqual(response.status_code, 200)
        response = yield from c.get(self.httpbin(), verify=crt)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.request.verify, crt)
