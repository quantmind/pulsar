from pulsar import test
from pulsar.http import HttpClient

from .helloworld import run

class TestCalculatorExample(test.TestCase):
    uri = 'http://localhost:8060'
    
    def setUp(self):
        self.c = HttpClient()
        
    def testThreadWorker(self):
        c = self.c
        app = run(worker_class = 'http_t')
        arbiter = app.arbiter
        self.assertEqual(arbiter.app,app)
        t = self.run_on_process(c.request,self.uri)
        resp = t.wait()
        content = resp.content
        self.assertEqual(content,'Hello World!\n')
