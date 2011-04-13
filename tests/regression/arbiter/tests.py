from pulsar import test, Arbiter, Application


class TestApp(Application):
    
    def init(self, parser, opts, args):
        pass

class TestArbiterClass(test.TestCase):
    
    def testArbiter(self):
        a = Arbiter(TestApp())
        self.assertTrue(isinstance(a.app,TestApp))