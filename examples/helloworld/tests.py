'''Tests the "helloworld" example.'''
from pulsar import send, SERVER_SOFTWARE, HttpClient, get_application
from pulsar import MultiDeferred
from pulsar.utils.httpurl import range
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
        name = name=cls.name()
        kwargs = {'%s__bind' % name: '127.0.0.1:0'}
        s = server(name=cls.name(), concurrency=cls.concurrency, **kwargs)
        outcome = send('arbiter', 'run', s)
        yield outcome
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            outcome = send('arbiter', 'kill_actor', cls.app.name)
            yield outcome
    
    @run_on_arbiter
    def testMeta(self):
        app = get_application(self.name())
        self.assertEqual(app.name, self.name())
        self.assertTrue(app.monitor.running())
        self.assertEqual(app, app.app)
        self.assertEqual(str(app), app.name)
        self.assertEqual(app.cfg.bind, '127.0.0.1:0')
        
    def testResponse(self):
        c = HttpClient()
        outcome = c.get(self.uri)
        yield outcome
        resp = outcome.result
        self.assertEqual(resp.status_code, 200)
        content = resp.content
        self.assertEqual(content, b'Hello World!\n')
        headers = resp.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'], 'text/plain')
        self.assertEqual(headers['server'], SERVER_SOFTWARE)
        
    def test_getbench(self):
        c = HttpClient()
        yield MultiDeferred((c.get(self.uri) for _ in range(1))).lock()
    test_getbench.__benchmark__ = True


@dont_run_with_thread
class TestHelloWorldProcess(TestHelloWorldThread):
    concurrency = 'process'
    