from pulsar.apps.test import unittest

from . import base


class TestHttpClient(base.TestHttpClientBase, unittest.TestCase):
# class TestHttpClient(base.TestHttpClient):

    def test_connect(self):
        http = self.client()
        response = yield http.connect('http://www.bbc.co.uk/')
        self.assertEqual(response.status)

