'''Tests the "helloworld" example.'''
from pulsar import send, SERVER_SOFTWARE, get_application
from pulsar import MultiDeferred
from pulsar.utils.pep import range
from pulsar.apps.http import HttpClient
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread

from .manage import server
        

class TestHelloWorldThread(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def name(cls):
        return 'helloworld_' + cls.concurrency
    
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.name(), concurrency=cls.concurrency,
                   bind='127.0.0.1:0')
        cls.app = yield send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.client = HttpClient()
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)
    
    @run_on_arbiter
    def testMeta(self):
        app = yield get_application(self.name())
        self.assertEqual(app.name, self.name())
        self.assertTrue(app.monitor.is_running())
        self.assertEqual(app, app.app)
        self.assertEqual(str(app), app.name)
        self.assertEqual(app.cfg.bind, '127.0.0.1:0')
        
    def testResponse(self):
        c = self.client
        response = yield c.get(self.uri).on_finished
        self.assertEqual(response.status_code, 200)
        content = response.get_content()
        self.assertEqual(content, b'Hello World!\n')
        headers = response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'], 'text/plain')
        self.assertEqual(headers['server'], SERVER_SOFTWARE)
    
    def testTimeIt(self):
        c = self.client
        response = c.timeit(5, 'get', self.uri)
        #cc = list(c.connection_pools.values())[0]._concurrent_connections
        #self.assertTrue(cc)
        yield response
        self.assertTrue(response.locked_time >= 0)
        self.assertTrue(response.total_time >= response.locked_time)
        self.assertEqual(response.num_failures, 0)
        
    def test_getbench(self):
        c = self.client
        yield MultiDeferred((c.get(self.uri) for _ in range(1))).lock()
    test_getbench.__benchmark__ = True


@dont_run_with_thread
class TestHelloWorldProcess(TestHelloWorldThread):
    concurrency = 'process'
    