'''Tests the rpc middleware and utilities'''
from pulsar.apps import rpc
from pulsar.utils.test import test


class rpcTest(test.TestCase):
    
    def testProxy(self):
        p = rpc.JsonProxy('http://127.0.0.1:8060',
                          timeout = 20)
        self.assertTrue(p._http)
        http = p._http
        self.assertTrue(http.headers)
        