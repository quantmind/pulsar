'''Tests the rpc middleware and utilities'''
import unittest as test

from pulsar.apps import rpc


class rpcTest(test.TestCase):
    
    def testProxy(self):
        p = rpc.JsonProxy('http://127.0.0.1:8060',
                          timeout = 20)
        self.assertTrue(p._http)
        http = p._http
        self.assertTrue(http.headers)
        