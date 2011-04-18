from pulsar import test, Arbiter, Application,\
                   PoolWorkerProxy, PoolProxy


class TestApp(Application):
    
    def init(self, parser, opts, args):
        pass

class TestArbiterClass(test.TestCase):
    
    def testArbiter(self):
        a = Arbiter(TestApp())
        self.assertTrue(isinstance(a.app,TestApp))
        
        
class TestProxies(test.TestCase):
    
    def testWorkerProxy(self):
        proxies = PoolWorkerProxy.proxy_functions
        self.assertEqual(len(proxies),2)
        
    def testPoolProxy(self):
        proxies = PoolProxy.proxy_functions
        self.assertEqual(len(proxies),1)