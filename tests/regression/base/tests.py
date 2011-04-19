from pulsar import test

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
    
    def testTestCase(self):
        self.assertTrue(self.suiterunner)
        r = self.suiterunner.ping()\
                .add_callback(lambda result : self.assertEqual(result,'pong'))
        self.assertEqual(len(r._callbacks),1)
        return r