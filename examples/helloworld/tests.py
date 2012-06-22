'''Tests the "helloworld" example.'''
from pulsar import SERVER_SOFTWARE, HttpClient
from pulsar.apps.test import test_server
from pulsar.utils.test import test

from .manage import server
        

class TestHelloWorldThread(test.TestCase):
    concurrency = 'thread'
    
    @classmethod
    def setUpClass(cls):
        name = 'helloworld_' + cls.concurrency
        s = test_server(server,
                        bind='127.0.0.1:0',
                        name=name,
                        concurrency=cls.concurrency)
        outcome = cls.worker.run_on_arbiter(s)
        yield outcome
        app = outcome.result
        cls.app = app
        cls.uri = 'http://{0}:{1}'.format(*app.address)
        
    @classmethod
    def tearDownClass(cls):
        return cls.worker.arbiter.send(cls.worker,'kill_actor',cls.app.mid)
    
    def testMeta(self):
        import pulsar
        name = 'helloworld_' + self.concurrency
        arbiter = pulsar.arbiter()
        self.assertTrue(len(arbiter.monitors)>=2)
        monitor = arbiter.monitors.get(name)
        self.assertEqual(monitor.name,name)
        self.assertTrue(monitor.running())
    testMeta.run_on_arbiter = True
        
    def testResponse(self):
        c = HttpClient()
        resp = c.request(self.uri)
        yield resp
        resp = resp.result
        self.assertTrue(resp.status_code,200)
        content = resp.content
        self.assertEqual(content,b'Hello World!\n')
        headers = resp.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'],'text/plain')
        self.assertEqual(headers['server'],SERVER_SOFTWARE)


class TestHelloWorldProcess(TestHelloWorldThread):
    concurrency = 'process'
    