'''Tests the "helloworld" example.'''
from pulsar import send, SERVER_SOFTWARE, HttpClient, arbiter
from pulsar.apps.test import unittest, run_on_arbiter

from .manage import server
        

class TestHelloWorldThread(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def setUpClass(cls):
        name = 'helloworld_' + cls.concurrency
        s = server(bind='127.0.0.1:0', name=name, concurrency=cls.concurrency)
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
        name = 'helloworld_' + self.concurrency
        a = arbiter()
        self.assertTrue(len(a.monitors)>=2)
        monitor = a.monitors.get(name)
        self.assertEqual(monitor.name,name)
        self.assertTrue(monitor.running())
        
    def testResponse(self):
        c = HttpClient()
        resp = c.get(self.uri)
        yield resp
        resp = resp.result
        self.assertTrue(resp.status_code, 200)
        content = resp.content
        self.assertEqual(content, b'Hello World!\n')
        headers = resp.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'],'text/plain')
        self.assertEqual(headers['server'], SERVER_SOFTWARE)


class TestHelloWorldProcess(TestHelloWorldThread):
    concurrency = 'process'
    