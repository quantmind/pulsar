'''Tests the "helloworld" example.'''
from pulsar import send, SERVER_SOFTWARE, HttpClient, get_application
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
        s = server(bind='127.0.0.1:0', name=cls.name(),
                   concurrency=cls.concurrency)
        outcome = send('arbiter', 'run', s)
        yield outcome
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            outcome = send('arbiter', 'kill_actor', cls.app.mid)
            yield outcome
    
    @run_on_arbiter
    def testMeta(self):
        app = get_application(self.name())
        self.assertEqual(app.name, self.name())
        self.assertTrue(app.monitor.running())
        
    def testResponse(self):
        c = HttpClient()
        outcome = c.get(self.uri)
        yield outcome
        resp = outcome.result
        self.assertTrue(resp.status_code, 200)
        content = resp.content
        self.assertEqual(content, b'Hello World!\n')
        headers = resp.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'], 'text/plain')
        self.assertEqual(headers['server'], SERVER_SOFTWARE)


@dont_run_with_thread
class TestHelloWorldProcess(TestHelloWorldThread):
    concurrency = 'process'
    