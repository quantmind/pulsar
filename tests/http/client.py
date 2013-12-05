from pulsar.apps.test import unittest

from . import base


class TestHttpClient(base.TestHttpClient):

    def test_connect(self):
        http = self.client()
        response = yield http.connect(self.app.address)
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response._request.method, 'CONNECT')


