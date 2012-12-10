'''Tests for arbiter and monitors.'''
import os
import time
from threading import current_thread

import pulsar
from pulsar import send, spawn
from pulsar.async.actor import ACTOR_STOPPING_LOOPS
from pulsar.apps.test import unittest, run_on_arbiter, ActorTestMixin


class BogusActor(pulsar.Actor):
    
    def __call__(self):
        #This actor does not send notify messages to the arbiter
        pass
    
    def stop(self, force=False, exit_code=0):
        if not self.params.failstop or\
            (self.params.failstop and force):
            return super(BogusActor, self).stop(force=force, exit_code=exit_code)
    
    
class TestArbiter(ActorTestMixin, unittest.TestCase):
    
    @run_on_arbiter
    def testSpawning(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertEqual(len(arbiter.monitors), 1)
        self.assertEqual(arbiter.monitors['test'].spawning_actors, {})
        yield self.spawn(name='foo')
        proxy = self.a
        self.assertEqual(proxy.name, 'foo')
        self.assertEqual(arbiter.spawning_actors, {})
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        arbiter.manage_actors()
        
    def testArbiter(self):
        worker = pulsar.get_actor()
        self.assertEqual(pulsar.arbiter(), None)
        arbiter = worker.arbiter
        self.assertTrue(arbiter)
        self.assertEqual(arbiter.name, 'arbiter')
        
    @run_on_arbiter
    def testArbiterObject(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        self.assertEqual(arbiter.impl.kind, 'monitor')
        self.assertTrue(arbiter.monitors)
        self.assertEqual(arbiter.ioloop, arbiter.requestloop)
        self.assertFalse(arbiter.cpubound)
        self.assertEqual(arbiter.exit_code, None)
        info = arbiter.info()
        self.assertTrue('server' in info)
        server = info['server']
        self.assertEqual(server['state'], 'running')
        
    @run_on_arbiter
    def testBadMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.monitors)
        name = list(arbiter.monitors.values())[0].name
        self.assertRaises(KeyError, arbiter.add_monitor, pulsar.Monitor, name)
        
    @run_on_arbiter
    def testTimeout(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        yield self.spawn(actor_class=BogusActor, name='foo', timeout=1)
        proxy = self.a
        self.assertEqual(proxy.name, 'foo')
        self.assertEqual(arbiter.spawning_actors, {})
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        self.assertEqual(proxy.stopping_loops, 0)
        time.sleep(1)
        n = arbiter.manage_actors()
        self.assertTrue(n)
        self.assertEqual(proxy.stopping_loops, 1)
        while proxy.aid in arbiter.managed_actors:
            yield pulsar.NOT_DONE
            arbiter.manage_actors()
        self.assertEqual(arbiter.manage_actors(), n-1)
        self.assertFalse(proxy.aid in arbiter.managed_actors)
        thread_actors = pulsar.process_local_data('thread_actors')
        self.assertFalse(proxy.aid in thread_actors)
        
    @run_on_arbiter
    def testTerminate(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        yield self.spawn(actor_class=BogusActor, name='foo', timeout=1,
                         failstop=True)
        proxy = self.a
        self.assertEqual(proxy.name, 'foo')
        proxy = arbiter.managed_actors[proxy.aid]
        self.assertEqual(proxy.stopping_loops, 0)
        time.sleep(1)
        n = arbiter.manage_actors()
        self.assertTrue(n)
        self.assertEqual(proxy.stopping_loops, 1)
        while proxy.aid in arbiter.managed_actors:
            yield pulsar.NOT_DONE
            arbiter.manage_actors()
        self.assertEqual(proxy.stopping_loops, ACTOR_STOPPING_LOOPS)
        thread_actors = pulsar.process_local_data('thread_actors')
        self.assertFalse(proxy.aid in thread_actors)
        

