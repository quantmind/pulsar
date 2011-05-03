'''Tests for arbiter an monitor
'''
from pulsar import test, Monitor, Actor, DEFAULT_MESSAGE_CHANNEL


class ActorA(Actor):
    
    def on_task(self):
        '''Ping the application monitor "app1"'''
        if not hasattr(self,'_result'):
            self._result = None
            self.log.debug('Pinging application 1')
            server = self.ACTOR_LINKS["app1"]
            self.proxy.ping(server).add_callback(self.fire_result)
        
    def fire_result(self, result):
        self.monitor.send(self.aid,result)
            


class TestArbiter(test.TestCase):
    
    def testArbiter(self):
        arbiter = self.arbiter
        self.assertEqual(arbiter.impl,'monitor')
        self.assertTrue(arbiter.monitors)
        self.assertTrue(arbiter.actor_functions)
        
class Testmonitor(test.TestCase):
    
    def _testcreatemonitor(self, impl):
        arbiter = self.arbiter
        m = arbiter.add_monitor(Monitor, Actor, 'testmonitor',
                                num_workers = 2,
                                actor_params = {'impl':impl})
        self.assertEqual(m.name,'testmonitor')
        self.assertTrue(isinstance(m,Monitor))
        self.assertEqual(m.worker_class,Actor)
        self.assertTrue(m.name in arbiter.monitors)
        self.assertEqual(m.num_workers,2)
        self.sleep(1)
        self.assertTrue(m.is_alive())
        self.assertEqual(m.ioloop,arbiter.ioloop)
        # Lets check the actors
        self.assertEqual(len(m.LIVE_ACTORS),2)
        m.stop()
        self.wait(lambda : m.name in arbiter.monitors)
        self.assertFalse(m.name in arbiter.monitors)
        self.assertFalse(m.is_alive())
        
    def _testMonitorLinks(self, impl):
        arbiter = self.arbiter
        m1 = arbiter.add_monitor(Monitor, Actor, 'app1',
                                 num_workers = 2,
                                 actor_params = {'impl':impl})
        m2 = arbiter.add_monitor(Monitor, ActorA, 'app2',
                                 num_workers = 2,
                                 actor_params = {'impl':impl})
        # app2 actors should have a link to the app1 applications
        links = self.arbiter.get_all_monitors()
        self.assertTrue('app1' in links)
        self.assertTrue('app2' in links)
        self.sleep(0.2)
        self.wait(lambda : m1.channels)
        for m in (m2,m1):
            m.stop()
            self.wait(lambda : m.name in arbiter.monitors)
            self.assertFalse(m.name in arbiter.monitors)
            self.assertFalse(m.is_alive())
        self.assertTrue(DEFAULT_MESSAGE_CHANNEL in m1.channels)
        
    def testCreateThreadMonitor(self):
        self._testcreatemonitor('thread')
        
    def testCreateProcessMonitor(self):
        self._testcreatemonitor('process')
        
    def testMonitorThreadLinks(self):
        self._testMonitorLinks('thread')
        
    def testMonitorProcessLinks(self):
        self._testMonitorLinks('process')
