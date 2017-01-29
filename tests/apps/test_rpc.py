'''Tests the rpc middleware and utilities. It uses the calculator example.'''
import unittest

from pulsar.apps import rpc
from pulsar.apps.test import HttpTestClient


class rpcTest(unittest.TestCase):

    def proxy(self):
        from examples.calculator.manage import Site
        http = HttpTestClient(self, Site())
        return rpc.JsonProxy('http://127.0.0.1:8060/', http=http, timeout=20)

    def test_proxy(self):
        p = self.proxy()
        http = p._http
        self.assertTrue(len(http.headers))
        self.assertEqual(http.headers['user-agent'], 'Pulsar-Http-Test-Client')
        self.assertTrue(http.wsgi)
        self.assertEqual(p._version, '2.0')

    async def test_addition(self):
        p = self.proxy()
        response = await p.calc.add(4, 5)
        self.assertEqual(response, 9)
