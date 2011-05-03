from pulsar import test, arbiter

import pulsar as package


class TestLibrary(test.TestCase):
    
    def test_version(self):
        self.assertTrue(package.VERSION)
        self.assertTrue(package.__version__)
        self.assertEqual(package.__version__,package.get_version())
        self.assertTrue(len(package.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(package, m, None))
            
            
class TestTestSuite(test.TestCase):
    '''Test the test suite itself'''
    def testTestSuite(self):
        self.assertTrue(self.suiterunner)
        self.assertEqual(self.arbiter,arbiter())
        self.cbk = self.Callback()
        #r = arb.ping(self.suiterunner).add_callback(self.cbk)
        #self.assertTrue(len(r._callbacks)==1)
        #self.assertEqual(self.cbk.result,'pong')
        
    def testTestMonitor(self):
        monitors = self.arbiter.get_all_monitors()
        self.assertTrue(monitors)
        self.assertTrue('testapplication' in monitors)
