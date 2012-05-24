import pulsar
from pulsar.utils.test import test

class TestTestWorker(test.TestCase):
    
    def testPulsarClient(self):
        actor = pulsar.get_actor()
        arbiter = actor.arbiter
        c = pulsar.PulsarClient(arbiter.address)
        self.assertEqual(c.ping(), 'pong')
        