from pulsar import test, SERVER_SOFTWARE
from pulsar.http import HttpClient

from .manage import server

class TestCalculatorExample(test.TestCase):
    uri = 'http://localhost:8060'
    
    def initTests(self):
        s = server(concurrency = 'process',parse_console = False)
        self.__class__._server = s
        s.start()
        monitor = self.arbiter.monitors[s.mid]
        self.wait(lambda : not monitor.is_alive())
        
    def endTests(self):
        monitor = self.arbiter.monitors[self._server.mid]
        monitor.stop()
        self.wait(lambda : monitor.aid in self.arbiter.monitors)
        self.assertFalse(monitor.is_alive())
        self.assertTrue(monitor.closed())
    
    def setUp(self):
        self.c = HttpClient()
        
    def testMonitors(self):
        s = self._server
        self.assertTrue(len(self.arbiter.monitors)>=2)
        self.assertTrue(s.mid in self.arbiter.monitors)
        monitor = self.arbiter.monitors[s.mid]
        self.assertTrue(monitor.is_alive())
        
    def testResponse(self):
        c = self.c
        resp = self.c.request(self.uri)
        self.assertTrue(resp.status,200)
        content = resp.content
        self.assertEqual(content,'Hello World!\n')
        headers = resp.response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'],'text/plain')
        self.assertEqual(headers['server'],SERVER_SOFTWARE)
