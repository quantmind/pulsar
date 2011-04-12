from pulsar import test, Arbiter, Application


class DummyApp(Application):
    pass


class TestArbiterClass(test.TestCase):
    
    def testArbiter(self):
        a = Arbiter(DummyApp())
        self.assertTrue(isinstance(a.app,DummyApp))