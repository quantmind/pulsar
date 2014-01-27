import unittest

from pulsar.utils.httpurl import urlparse

from . import base


class TestHttpClient(base.TestHttpClient):

    def __test_connect(self):
        http = self.client()
        p = urlparse(self.uri)
        response = yield http.connect(p.netloc)
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response._request.method, 'CONNECT')
