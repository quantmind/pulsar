'''Tests the RPC "calculator" example.'''
import unittest as test

from pulsar.apps import rpc
from pulsar.apps.test import test_server

from .manage import server


class TestRpcThread(test.TestCase):
    concurrency = 'thread'
    
    @classmethod
    def setUpClass(cls):
        name = 'calc_' + cls.concurrency
        s = test_server(server,
                        bind = '127.0.0.1:0',
                        name = name,
                        concurrency = cls.concurrency)
        r,outcome = cls.worker.run_on_arbiter(s)
        yield r
        app = outcome.result
        cls.app = app
        cls.uri = 'http://{0}:{1}'.format(*app.address)
        
    @classmethod
    def tearDownClass(cls):
        return cls.worker.arbiter.send(cls.worker,'kill_actor',cls.app.mid)
        
    def setUp(self):
        self.p = rpc.JsonProxy(self.uri)
        
    def testHandler(self):
        s = self.app
        self.assertTrue(s.callable)
        middleware = s.callable
        root = middleware.handler
        self.assertEqual(root.content_type,'text/json')
        self.assertEqual(middleware.path,'/')
        self.assertTrue(middleware.raise404)
        self.assertEqual(len(root.subHandlers),1)
        hnd = root.subHandlers['calc']
        self.assertFalse(hnd.isroot())
        self.assertEqual(hnd.subHandlers,{})
        self.assertTrue(s.mid)
        
    def testPing(self):
        result = self.p.ping()
        self.assertEqual(result,'pong')
        
    def testAdd(self):
        result = self.p.calc.add(3,7)
        self.assertEqual(result,10)
        
    def testSubtract(self):
        result = self.p.calc.subtract(546,46)
        self.assertEqual(result,500)
        
    def testMultiply(self):
        result = self.p.calc.multiply(3,9)
        self.assertEqual(result,27)
        
    def testDivide(self):
        result = self.p.calc.divide(50,25)
        self.assertEqual(result,2)
        
    def testInfo(self):
        result = self.p.server_info()
        self.assertTrue('server' in result)
        server = result['server']
        self.assertTrue('version' in server)
        
    def testInvalidParams(self):
        self.assertRaises(rpc.InvalidParams,self.p.calc.divide,50,25,67)
        
    def testInvalidFunction(self):
        self.assertRaises(rpc.NoSuchFunction,self.p.foo,'ciao')
        
    def testInternalError(self):
        self.assertRaises(rpc.InternalError,self.p.calc.divide,'ciao','bo')
        

class TestRpcProcess(TestRpcThread):
    concurrency = 'process'