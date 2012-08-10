'''Tests the rpc middleware and utilities'''
from pulsar.apps import rpc
from pulsar.apps.test import unittest


class rpcTest(unittest.TestCase):

    def testProxy(self):
        p = rpc.JsonProxy('http://127.0.0.1:8060', timeout=20)
        self.assertEqual(p.http.timeout, 20)
        http = p.http
        self.assertTrue(len(http.headers))
        self.assertNotEqual(http.headers['user-agent'], 'testing')
        http.headers['user-agent'] = 'testing'
        self.assertEqual(http.headers['user-agent'], 'testing')
