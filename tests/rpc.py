'''Tests the rpc middleware and utilities. It uses the calculator example.'''
from pulsar.apps import rpc
from pulsar.apps.test import unittest, HttpTestClient


class rpcTest(unittest.TestCase):

    def proxy(self):
        from examples.calculator.manage import wsgi_handler
        http = HttpTestClient(self, wsgi_handler())
        return rpc.JsonProxy('http://127.0.0.1:8060/', http=http, timeout=20)

    def testProxy(self):
        p = self.proxy()
        http = p.http
        self.assertTrue(len(http.headers))
        self.assertEqual(http.headers['user-agent'], 'Pulsar-Http-Test-Client')
        self.assertEqual(http.test, self)

    def testInvalidFunction(self):
        from pulsar.apps.rpc import NoSuchFunction
        p = self.proxy()
        self.assertRaises(NoSuchFunction, p.blabla)
        self.assertRaises(NoSuchFunction, p.blabla.foofoo)
        self.assertRaises(NoSuchFunction, p.blabla.foofoo.sjdcbjcb)
        try:
            p.blabla()
        except NoSuchFunction as e:
            self.assertEqual(str(e),
                '''NoSuchFunction -32601: Function "blabla" not available.''')
        try:
            p.blabla.foofoo()
        except NoSuchFunction as e:
            self.assertEqual(str(e),
                '''NoSuchFunction -32601: Function "blabla.foofoo" not available.''')

