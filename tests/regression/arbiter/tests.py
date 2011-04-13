from pulsar import test, Arbiter, Application


class TestArbiterClass(test.TestCase):
    
    def testArbiter(self):
        a = Arbiter(Application())
        self.assertTrue(isinstance(a.app,Application))