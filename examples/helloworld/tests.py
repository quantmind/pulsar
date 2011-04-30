from pulsar import test, SERVER_SOFTWARE, Queue, Empty
from pulsar.http import HttpClient

from .manage import server


class PostRequest(object):

    def __init__(self):
        self.q = Queue()
        
    def __call__(self, worker, request):
        self.q.put((worker.aid,worker.age,worker.nr))
        
    def get(self):
        try:
            return self.q.get(timeout = 0.5)
        except Empty:
            return None
        

class TestCalculatorExample(test.TestCase):
    uri = 'http://localhost:8060'
    
    def initTests(self):
        r = PostRequest()
        s = server(concurrency = 'process',parse_console = False,post_request=r)
        self.__class__._server = s
        self.__class__._rm = r
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
        r = self._rm
        resp = self.c.request(self.uri)
        self.assertTrue(resp.status,200)
        content = resp.content
        self.assertEqual(content,b'Hello World!\n')
        headers = resp.response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'],'text/plain')
        self.assertEqual(headers['server'],SERVER_SOFTWARE)
        #
        # lets check the response count
        aid,age,nr = r.get()
        self.assertTrue(nr)
        resp = self.c.request(self.uri)
        aid1,age1,nr1 = r.get()
        if aid == aid1:
            self.assertEqual(nr1,nr+1)
        
