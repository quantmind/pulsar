from pulsar import test


class TestArbiter(test.TestCase):
    
    def testArbiter(self):
        arbiter = self.arbiter
        self.assertEqual(arbiter.impl,'monitor')
        self.assertTrue(arbiter.monitors)
        self.assertTrue(arbiter.actor_functions)
        