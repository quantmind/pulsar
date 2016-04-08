'''Tests the RPC "calculator" example.'''
import unittest
import types

from pulsar import send
from pulsar.apps import rpc, http
from pulsar.apps.test import dont_run_with_thread

from examples.calculator.manage import server, Root, Calculator


class TestRpcOnThread(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500

    @classmethod
    async def setUpClass(cls):
        name = 'calc_' + cls.concurrency
        s = server(bind='127.0.0.1:0', name=name, concurrency=cls.concurrency)
        cls.app_cfg = await send('arbiter', 'run', s)
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
    async def test_ping(self):
        response = await self.p.ping()
        self.assertEqual(response, 'pong')

    async def test_functions_list(self):
        result = await self.p.functions_list()
        self.assertTrue(result)
        d = dict(result)
        self.assertTrue('ping' in d)
        self.assertTrue('echo' in d)
        self.assertTrue('functions_list' in d)
        self.assertTrue('calc.add' in d)
        self.assertTrue('calc.divide' in d)

    async def test_time_it(self):
        '''Ping server 5 times'''
        bench = await self.p.timeit('ping', 5)
        self.assertTrue(len(bench.result), 5)
        self.assertTrue(bench.taken)

    # Test Object method
    async def test_check_request(self):
        result = await self.p.check_request('check_request')
        self.assertTrue(result)

    async def test_add(self):
        response = await self.p.calc.add(3, 7)
        self.assertEqual(response, 10)

    async def test_subtract(self):
        response = await self.p.calc.subtract(546, 46)
        self.assertEqual(response, 500)

    async def test_multiply(self):
        response = await self.p.calc.multiply(3, 9)
        self.assertEqual(response, 27)

    async def test_divide(self):
        response = await self.p.calc.divide(50, 25)
        self.assertEqual(response, 2)

    async def test_info(self):
        response = await self.p.server_info()
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
        return self.wait.assertRaises(rpc.InvalidParams, self.p.calc.add,
                                      50, 25, 67)

    def test_invalid_params_fromApi(self):
        return self.wait.assertRaises(rpc.InvalidParams, self.p.calc.divide,
                                      50, 25, 67)

    async def test_invalid_function(self):
        p = self.p
        await self.wait.assertRaises(rpc.NoSuchFunction, p.foo, 'ciao')
        await self.wait.assertRaises(rpc.NoSuchFunction,
                                     p.blabla)
        await self.wait.assertRaises(rpc.NoSuchFunction,
                                     p.blabla.foofoo)
        await self.wait.assertRaises(rpc.NoSuchFunction,
                                     p.blabla.foofoo.sjdcbjcb)

    def testInternalError(self):
        return self.wait.assertRaises(rpc.InternalError, self.p.calc.divide,
                                      'ciao', 'bo')

    def testCouldNotserialize(self):
        return self.wait.assertRaises(rpc.InternalError, self.p.dodgy_method)

    async def testpaths(self):
        '''Fetch a sizable ammount of data'''
        response = await self.p.calc.randompaths(num_paths=20, size=100,
                                                 mu=1, sigma=2)
        self.assertTrue(response)

    async def test_echo(self):
        response = await self.p.echo('testing echo')
        self.assertEqual(response, 'testing echo')

    async def test_docs(self):
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
        response = await self.p.documentation()
        self.assertEqual(response, docs)

    async def test_batch_one_call(self):
        bp = rpc.JsonBatchProxy(self.uri, timeout=self.rpc_timeout)

        call_id1 = bp.ping()
        self.assertIsNotNone(call_id1)
        self.assertEqual(len(bp), 1)

        batch_generator = await bp()
        self.assertIsInstance(batch_generator, types.GeneratorType)
        self.assertEqual(len(bp), 0)

        for ind, batch_response in enumerate(batch_generator):
            self.assertEqual(ind, 0)
            self.assertEqual(call_id1, batch_response.id)
            self.assertEqual(batch_response.result, 'pong')
            self.assertIsNone(batch_response.exception)

    async def test_batch_few_call(self):
        bp = rpc.JsonBatchProxy(self.uri, timeout=self.rpc_timeout)

        call_id1 = bp.ping()
        self.assertIsNotNone(call_id1)
        self.assertEqual(len(bp), 1)

        call_id2 = bp.calc.add(1, 1)
        self.assertIsNotNone(call_id2)
        self.assertEqual(len(bp), 2)

        batch_generator = await bp()
        self.assertIsInstance(batch_generator, types.GeneratorType)
        self.assertEqual(len(bp), 0)

        for ind, batch_response in enumerate(batch_generator):
            self.assertIn(ind, (0, 1))
            if call_id1 == batch_response.id:
                self.assertEqual(batch_response.result, 'pong')
                self.assertIsNone(batch_response.exception)
            elif call_id2 == batch_response.id:
                self.assertEqual(batch_response.result, 2)
                self.assertIsNone(batch_response.exception)

    async def test_batch_error_response_call(self):
        bp = rpc.JsonBatchProxy(self.uri, timeout=self.rpc_timeout)

        call_id1 = bp.ping('wrong param')
        self.assertIsNotNone(call_id1)
        self.assertEqual(len(bp), 1)

        batch_generator = await bp()
        self.assertIsInstance(batch_generator, types.GeneratorType)
        self.assertEqual(len(bp), 0)

        for ind, batch_response in enumerate(batch_generator):
            self.assertEqual(ind, 0)
            self.assertEqual(call_id1, batch_response.id)
            self.assertIsInstance(batch_response.exception, rpc.InvalidParams)
            self.assertIsNone(batch_response.result)

    async def test_batch_full_response_call(self):
        bp = rpc.JsonBatchProxy(self.uri, timeout=self.rpc_timeout,
                                full_response=True)

        bp.ping()
        bp.ping()
        bp.ping()
        self.assertEqual(len(bp), 3)

        response = await bp()
        self.assertIsInstance(response, http.HttpResponse)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(bp), 0)


@dont_run_with_thread
class TestRpcOnProcess(TestRpcOnThread):
    concurrency = 'process'

    # Synchronous client
    def test_sync_ping(self):
        sync = rpc.JsonProxy(self.uri, sync=True)
        self.assertEqual(sync.ping(), 'pong')
        self.assertEqual(sync.ping(), 'pong')
