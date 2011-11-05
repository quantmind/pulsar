'''Tests for arbiter and monitors.'''
import unittest as test
import os

import pulsar


class ActorA(pulsar.Actor):
    '''This actor ping application "app1" and once it receive the callback
it fires a message with the result to its monitor.'''
    def on_task(self):
        '''Ping the application monitor "app1"'''
        if not hasattr(self,'_result'):
            self._result = None
            self.log.debug('{0} pinging "app1"'.format(self))
            server = self.ACTOR_LINKS["app1"]
            self.proxy.ping(server).add_callback(self.fire_result)
        
    def fire_result(self, result):
        self.monitor.send(self.aid,result)
        

class TestArbiter(test.TestCase):
    
    def testArbiter(self):
        worker = self.worker
        if worker.cfg.concurrency == 'process':
            self.assertRaises(pulsar.PulsarException,pulsar.arbiter)
        elif worker.cfg.concurrency == 'thread':
            # In thread mode, the arbiter is accessible
            a = pulsar.arbiter()
            self.assertNotEqual(worker.tid,a.tid)
            self.assertEqual(worker.pid,a.pid)
        arbiter = worker.arbiter
        self.assertTrue(arbiter)
        
    def testArbiterObject(self):
        arbiter = pulsar.arbiter()
        self.assertTrue(arbiter.is_arbiter())
        self.assertEqual(arbiter.impl,'monitor')
        self.assertTrue(arbiter.monitors)
    testArbiterObject.run_on_arbiter = True
        
        
class TestMonitor(object):
    
    def get_monitor(self,impl,name,actor=None):
        actor = actor or pulsar.Actor
        m = self.arbiter.add_monitor(pulsar.Monitor, actor, name,
                                     num_workers = 2,
                                     actor_params = {'impl':impl})
        self.assertEqual(m.ioloop,self.arbiter.ioloop)
        self.sleep(0.2)
        self.assertTrue(m.is_alive())
        self.assertEqual(m.name,name)
        self.assertTrue(isinstance(m,Monitor))
        self.assertEqual(m.worker_class,actor)
        return m
    
    def stop_monitor(self, m):
        arbiter = self.arbiter
        self.assertEqual(len(m.LIVE_ACTORS),2)
        m.stop()
        self.wait(lambda : m.name in arbiter.monitors)
        self.assertFalse(m.name in arbiter.monitors)
        self.assertFalse(m.is_alive())
        
    def _testcreatemonitor(self, impl):
        '''Create a monitor with two actors and ping it from the arbiter.'''
        m = self.get_monitor(impl, 'testmonitor')
        self.sleep(1)
        cbk = self.Callback()
        self.arbiter.proxy.ping(m.proxy).add_callback(cbk)
        self.wait(lambda : not hasattr(cbk,'result'))
        self.assertEqual(cbk.result,"pong")
        self.stop_monitor(m)
        self.sleep(1)
        
    def _testMonitorLinks(self, impl):
        '''an actor in "app2" ping "app1" and when it receive the
pong callback it send a message to the "app2" monitor'''
        m1 = self.get_monitor(impl, "app1")
        m2 = self.get_monitor(impl, "app2", ActorA)
        # app2 actors should have a link to the app1 applications
        links = self.arbiter.get_all_monitors()
        self.assertTrue('app1' in links)
        self.assertTrue('app2' in links)
        self.sleep(0.2)
        self.wait(lambda : not m2.channels)
        for m in (m2,m1):
            m.stop()
            self.wait(lambda : m.name in self.arbiter.monitors)
            self.assertFalse(m.name in self.arbiter.monitors)
            self.assertFalse(m.is_alive())
        self.assertTrue(DEFAULT_MESSAGE_CHANNEL in m2.channels)
        r = m2.channels[DEFAULT_MESSAGE_CHANNEL].pop()
        self.assertEqual(r.msg,'pong')
        
    def testCreateThreadMonitor(self):
        self._testcreatemonitor('thread')
        
    def testCreateProcessMonitor(self):
        self._testcreatemonitor('process')
        
    def testMonitorThreadLinks(self):
        self._testMonitorLinks('thread')
        
    def testMonitorProcessLinks(self):
        self._testMonitorLinks('process')
