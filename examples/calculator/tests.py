'''Tests the RPC "calculator" example.'''
from pulsar import send
from pulsar.apps import rpc
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server


class TestRpcOnThread(unittest.TestCase):
    app = None
    concurrency = 'thread'
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500
    
    @classmethod
    def setUpClass(cls):
        name = 'calc_' + cls.concurrency
        s = server(bind='127.0.0.1:0', name=name, concurrency=cls.concurrency)
        cls.app = yield send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.p = rpc.JsonProxy(cls.uri, timeout=cls.rpc_timeout)
        cls.sync = rpc.JsonProxy(cls.uri, force_sync=True)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app:
            return send('arbiter', 'kill_actor', cls.app.name)
        
    def setUp(self):
        self.assertEqual(self.p.url, self.uri)
        self.assertTrue(str(self.p))
        proxy = self.p.bla
        self.assertEqual(proxy.name, 'bla')
        self.assertEqual(proxy.url, self.uri)
        self.assertEqual(proxy._client, self.p)
        self.assertEqual(str(proxy), 'bla')
        
    def test_handler(self):
        s = self.app
        self.assertTrue(s.callable)
        wsgi_handler = s.callable.handler
        self.assertEqual(len(wsgi_handler.middleware), 1)
        router = wsgi_handler.middleware[0]
        self.assertEqual(router.route.path, '/')
        root = router.post
        self.assertEqual(len(root.subHandlers), 1)
        hnd = root.subHandlers['calc']
        self.assertFalse(hnd.isroot())
        self.assertEqual(hnd.subHandlers, {})
        
    # Pulsar server commands
    def test_ping(self):
        response = yield self.p.ping()
        self.assertEqual(response, 'pong')
        
    def test_functions_list(self):
        result = yield self.p.functions_list()
        self.assertTrue(result)
        d = dict(result)
        self.assertTrue('ping' in d)
        self.assertTrue('echo' in d)
        self.assertTrue('functions_list' in d)
        self.assertTrue('calc.add' in d)
        self.assertTrue('calc.divide' in d)
        
    def test_time_it(self):
        '''Ping server 20 times'''
        response = self.p.timeit('ping', 20)
        yield response
        self.assertTrue(response.locked_time > 0)
        self.assertTrue(response.total_time >= response.locked_time)
        self.assertEqual(response.num_failures, 0)
        
    # Test Object method
    def test_check_request(self):
        result = yield self.p.check_request('check_request')
        self.assertTrue(result)
        
    def testAdd(self):
        response = yield self.p.calc.add(3,7)
        self.assertEqual(response, 10)
        
    def testSubtract(self):
        response = yield self.p.calc.subtract(546, 46)
        self.assertEqual(response, 500)
        
    def testMultiply(self):
        response = yield self.p.calc.multiply(3, 9)
        self.assertEqual(response, 27)
        
    def testDivide(self):
        response = yield self.p.calc.divide(50, 25)
        self.assertEqual(response, 2)
        
    def testInfo(self):
        response = yield self.p.server_info()
        self.assertTrue('server' in response)
        server = response['server']
        self.assertTrue('version' in server)
        
    def testInvalidParams(self):
        self.async.assertRaises(rpc.InvalidParams, self.p.calc.add, 50, 25, 67)
        
    def testInvalidParamsFromApi(self):
        self.async.assertRaises(rpc.InvalidParams, self.p.calc.divide,
                                50, 25, 67)
        
    def test_invalid_function(self):
        p = self.p
        yield self.async.assertRaises(rpc.NoSuchFunction, p.foo, 'ciao')
        yield self.async.assertRaises(rpc.NoSuchFunction,
                                      p.blabla)
        yield self.async.assertRaises(rpc.NoSuchFunction,
                                      p.blabla.foofoo)
        yield self.async.assertRaises(rpc.NoSuchFunction,
                                      p.blabla.foofoo.sjdcbjcb)


        
    def testInternalError(self):
        self.async.assertRaises(rpc.InternalError, self.p.calc.divide,
                                'ciao', 'bo')
        
    def testCouldNotserialize(self):
        self.async.assertRaises(rpc.InternalError, self.p.dodgy_method)
        
    def testpaths(self):
        '''Fetch a sizable ammount of data'''
        response = yield self.p.calc.randompaths(num_paths=20, size=100,
                                                 mu=1, sigma=2)
        self.assertTrue(response)
        
    # Synchronous client
    def test_sync_ping(self):
        self.assertEqual(self.sync.ping(), 'pong')
        self.assertEqual(self.sync.ping(), 'pong')


@dont_run_with_thread
class TestRpcOnProcess(TestRpcOnThread):
    concurrency = 'process'