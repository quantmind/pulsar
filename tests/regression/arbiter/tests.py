'''Tests for arbiter an monitor
'''
from pulsar import test, Monitor, Actor


class TestArbiter(test.TestCase):
    
    def testArbiter(self):
        arbiter = self.arbiter
        self.assertEqual(arbiter.impl,'monitor')
        self.assertTrue(arbiter.monitors)
        self.assertTrue(arbiter.actor_functions)
        
class Testmonitor(test.TestCase):
    
    def _testcreatemonitor(self, impl):
        arbiter = self.arbiter
        m = arbiter.add_monitor(Monitor, Actor, num_workers = 2,
                                actor_params = {'impl':impl})
        self.assertTrue(isinstance(m,Monitor))
        self.assertEqual(m.worker_class,Actor)
        self.assertTrue(m.aid in arbiter.monitors)
        self.assertEqual(m.num_workers,2)
        self.sleep(1)
        self.assertTrue(m.is_alive())
        self.assertEqual(m.ioloop,arbiter.ioloop)
        # Lets check the actors
        self.assertEqual(len(m.LIVE_ACTORS),2)
        m.stop()
        self.wait(lambda : m.aid in arbiter.monitors)
        self.assertFalse(m.aid in arbiter.monitors)
        self.assertFalse(m.is_alive())
        
    def testCreateThreadMonitor(self):
        self._testcreatemonitor('thread')
        
    def testCreateProcessMonitor(self):
        self._testcreatemonitor('process')