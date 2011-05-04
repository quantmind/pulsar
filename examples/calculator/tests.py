from pulsar import test
from pulsar.http import rpc

from .manage import server

class TestRpc(test.TestCase):
    
    def initTests(self):
        s = self.__class__._server = server(bind = '127.0.0.1:0',
                                            concurrency = 'process',
                                            parse_console = False)
        s.start()
        monitor = self.arbiter.get_monitor(s.mid)
        self.wait(lambda : not monitor.is_alive())
        self.__class__.address = 'http://{0}:{1}'.format(*monitor.address)
        
    def endTests(self):
        monitor = self.arbiter.get_monitor(self._server.mid)
        monitor.stop()
        self.wait(lambda : monitor.aid in self.arbiter.monitors)
        self.assertFalse(monitor.is_alive())
        self.assertTrue(monitor.closed())
        
    def setUp(self):
        self.p = rpc.JsonProxy(self.__class__.address)
        
    def testHandler(self):
        s = self._server
        self.assertTrue(s.callable)
        handler = s.callable
        self.assertEqual(handler.content_type,'text/json')
        self.assertEqual(handler.route,'/')
        self.assertEqual(len(handler.subHandlers),1)
        self.assertTrue(s.mid)
        monitor = self.arbiter.get_monitor(s.mid)
        socket = monitor.socket
        self.assertTrue(socket)
        self.assertTrue(monitor.address)
        
    def testPing(self):
        result = self.p.ping()
        self.assertEqual(result,'pong')
        
    def testAdd(self):
        result = self.p.calc.add(3,7)
        self.assertEqual(result,10)
        
    def testSubtract(self):
        result = self.p.calc.subtract(546,46)
        self.assertEqual(result,500)
        
    def testMultiply(self):
        result = self.p.calc.multiply(3,9)
        self.assertEqual(result,27)
        
    def testDivide(self):
        result = self.p.calc.divide(50,25)
        self.assertEqual(result,2)
        
    def testInfo(self):
        result = self.p.server_info()
        self.assertTrue('server' in result)
        server = result['server']
        self.assertTrue('version' in server)
        
    def testInvalidParams(self):
        self.assertRaises(rpc.InvalidParams,self.p.calc.divide,50,25,67)
        
    def testInvalidFunction(self):
        self.assertRaises(rpc.NoSuchFunction,self.p.foo,'ciao')
        

