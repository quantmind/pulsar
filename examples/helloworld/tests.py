from pulsar import test
from pulsar.http import HttpClient

from .helloworld import run

class TestCalculatorExample(test.TestCase):
    uri = 'http://localhost:8060'
    
    def setUp(self):
        self.c = HttpClient()
        
    def testThreadWorker(self):
        run(worker_class = 'http_t')
        resp = self.c.request(self.uri)
        self.assertEqual(resp,'Hello World!')
