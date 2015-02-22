'''Tests the RPC "calculator" example.'''
import unittest

from pulsar import send
from pulsar.apps import rpc
from pulsar.apps.test import dont_run_with_thread

from .manage import server, Root, Calculator


class TestRpcOnThread(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500

    @classmethod
    def setUpClass(cls):
        name = 'calc_' + cls.concurrency
        s = server(bind='127.0.0.1:0', name=name, concurrency=cls.concurrency)
        cls.app_cfg = yield from send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app_cfg.addresses[0])
        cls.p = rpc.JsonProxy(cls.uri, timeout=cls.rpc_timeout)

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    def setUp(self):
        self.assertEqual(self.p.url, self.uri)
        self.assertTrue(str(self.p))
        proxy = self.p.bla
        self.assertEqual(proxy.name, 'bla')
        self.assertEqual(proxy.url, self.uri)
        self.assertEqual(proxy._client, self.p)
        self.assertEqual(str(proxy), 'bla')

    def test_wsgi_handler(self):
        cfg = self.app_cfg
        self.assertTrue(cfg.callable)
        wsgi_handler = cfg.callable.setup({})
        self.assertEqual(len(wsgi_handler.middleware), 2)
        router = wsgi_handler.middleware[1]
        self.assertEqual(router.route.path, '/')
        root = router.post
        self.assertEqual(len(root.subHandlers), 1)
        hnd = root.subHandlers['calc']
        self.assertFalse(hnd.isroot())
        self.assertEqual(hnd.subHandlers, {})

    # Pulsar server commands
    def test_ping(self):
        response = yield from self.p.ping()
        self.assertEqual(response, 'pong')

    def test_functions_list(self):
        result = yield from self.p.functions_list()
        self.assertTrue(result)
        d = dict(result)
        self.assertTrue('ping' in d)
        self.assertTrue('echo' in d)
        self.assertTrue('functions_list' in d)
        self.assertTrue('calc.add' in d)
        self.assertTrue('calc.divide' in d)

    def test_time_it(self):
        '''Ping server 5 times'''
        bench = yield from self.p.timeit('ping', 5)
        self.assertTrue(len(bench.result), 5)
        self.assertTrue(bench.taken)

    # Test Object method
    def test_check_request(self):
        result = yield from self.p.check_request('check_request')
        self.assertTrue(result)

    def test_add(self):
        response = yield from self.p.calc.add(3, 7)
        self.assertEqual(response, 10)

    def test_subtract(self):
        response = yield from self.p.calc.subtract(546, 46)
        self.assertEqual(response, 500)

    def test_multiply(self):
        response = yield from self.p.calc.multiply(3, 9)
        self.assertEqual(response, 27)

    def test_divide(self):
        response = yield from self.p.calc.divide(50, 25)
        self.assertEqual(response, 2)

    def test_info(self):
        response = yield from self.p.server_info()
        self.assertTrue('server' in response)
        server = response['server']
        self.assertTrue('version' in server)
        app = response['monitors'][self.app_cfg.name]
        if self.concurrency == 'thread':
            self.assertFalse(app['workers'])
            worker = app
        else:
            workers = app['workers']
            self.assertEqual(len(workers), 1)
            worker = workers[0]
        name = '%sserver' % self.app_cfg.name
        if name in worker:
            self._check_tcpserver(worker[name]['server'])

    def _check_tcpserver(self, server):
        sockets = server['sockets']
        if sockets:
            self.assertEqual(len(sockets), 1)
            sock = sockets[0]
            self.assertEqual(sock['address'],
                             '%s:%s' % self.app_cfg.addresses[0])

    def test_invalid_params(self):
        return self.async.assertRaises(rpc.InvalidParams, self.p.calc.add,
                                       50, 25, 67)

    def test_invalid_params_fromApi(self):
        return self.async.assertRaises(rpc.InvalidParams, self.p.calc.divide,
                                       50, 25, 67)

    def test_invalid_function(self):
        p = self.p
        yield from self.async.assertRaises(rpc.NoSuchFunction, p.foo, 'ciao')
        yield from self.async.assertRaises(rpc.NoSuchFunction,
                                           p.blabla)
        yield from self.async.assertRaises(rpc.NoSuchFunction,
                                           p.blabla.foofoo)
        yield from self.async.assertRaises(rpc.NoSuchFunction,
                                           p.blabla.foofoo.sjdcbjcb)

    def testInternalError(self):
        return self.async.assertRaises(rpc.InternalError, self.p.calc.divide,
                                       'ciao', 'bo')

    def testCouldNotserialize(self):
        return self.async.assertRaises(rpc.InternalError, self.p.dodgy_method)

    def testpaths(self):
        '''Fetch a sizable ammount of data'''
        response = yield from self.p.calc.randompaths(num_paths=20, size=100,
                                                      mu=1, sigma=2)
        self.assertTrue(response)

    def test_echo(self):
        response = yield from self.p.echo('testing echo')
        self.assertEqual(response, 'testing echo')

    def test_docs(self):
        handler = Root({'calc': Calculator})
        self.assertEqual(handler.parent, None)
        self.assertEqual(handler.root, handler)
        self.assertRaises(rpc.NoSuchFunction, handler.get_handler,
                          'cdscsdcscd')
        calc = handler.subHandlers['calc']
        self.assertEqual(calc.parent, handler)
        self.assertEqual(calc.root, handler)
        docs = handler.docs()
        self.assertTrue(docs)
        response = yield from self.p.documentation()
        self.assertEqual(response, docs)


@dont_run_with_thread
class TestRpcOnProcess(TestRpcOnThread):
    concurrency = 'process'

    # Synchronous client
    def test_sync_ping(self):
        sync = rpc.JsonProxy(self.uri, sync=True)
        self.assertEqual(sync.ping(), 'pong')
        self.assertEqual(sync.ping(), 'pong')
