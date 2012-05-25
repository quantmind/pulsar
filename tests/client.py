import pulsar
from pulsar.utils.test import test

# you need to pass functions, you cannot pass lambdas
def testrun(actor):
    return actor.aid


class TestPulsarClient(test.TestCase):
    
    def client(self):
        actor = pulsar.get_actor()
        arbiter = actor.arbiter
        return pulsar.PulsarClient(arbiter.address)
        
    def testPing(self):
        c = self.client()
        self.assertEqual(c.ping(), 'pong')
        
    def testRun(self):
        c = self.client()
        result = c.run(testrun)
        self.assertEqual(result, 'arbiter')
        
    def testInfo(self):
        c = self.client()
        info = c.info()
        self.assertTrue(info)
        self.assertEqual(len(info['monitors']), 1)
        self.assertEqual(info['monitors'][0]['name'], 'test')
        
    def testShutDown(self):
        c = self.client()
        c.shutdown()
        