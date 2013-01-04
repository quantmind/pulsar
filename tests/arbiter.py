'''Tests for arbiter and monitors.'''
import os
import time
from threading import current_thread

import pulsar
from pulsar import send, spawn
from pulsar.utils import system
from pulsar.async.actor import ACTOR_STOPPING_LOOPS, EXIT_SIGNALS
from pulsar.apps.test import unittest, run_on_arbiter, ActorTestMixin, dont_run_with_thread


class BogusActor(pulsar.Actor):
    
    def notify(self):
        #This actor does not send notify messages to the arbiter
        if not self.params.last_notified:
            super(BogusActor, self).notify()
        else:
            pass
    
    def stop(self, force=False, exit_code=0):
        # override stop method so that no stopping can take place
        pass
    
    
class TestArbiterThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'
    
    @run_on_arbiter
    def testSpawning(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertTrue(len(arbiter.monitors) >= 1)
        future = spawn(name='testSpawning', concurrency=self.concurrency)
        self.assertTrue(future.aid in arbiter.spawning_actors)
        self.assertFalse(future.aid in arbiter.managed_actors)
        yield future
        proxy = future.result
        self.assertEqual(future.aid, proxy.aid)
        self.assertFalse(future.aid in arbiter.spawning_actors)
        self.assertEqual(proxy.name, 'testSpawning')
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        yield send(proxy, 'stop')
        
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
        self.assertEqual(arbiter.on_event(None, None), None)
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
        self.assertFalse(proxy.aid in arbiter.spawning_actors)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        self.assertEqual(proxy.stopping_loops, 0)
        time.sleep(1.5)
        self.assertTrue(arbiter.manage_actors())
        self.assertEqual(proxy.stopping_loops, 1)
        c = 0
        while c<20 and proxy.aid in arbiter.managed_actors:
            c += 1
            yield pulsar.NOT_DONE
            arbiter.manage_actors()
        self.assertFalse(proxy.aid in arbiter.managed_actors)
        thread_actors = pulsar.process_local_data('thread_actors')
        self.assertFalse(proxy.aid in thread_actors)
        
    @run_on_arbiter
    def testTerminate(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        yield self.spawn(actor_class=BogusActor, name='foo', timeout=1)
        proxy = self.a
        self.assertEqual(proxy.name, 'foo')
        proxy = arbiter.managed_actors[proxy.aid]
        self.assertEqual(proxy.stopping_loops, 0)
        time.sleep(1.5)
        n = arbiter.manage_actors()
        self.assertTrue(n)
        self.assertEqual(proxy.stopping_loops, 1)
        c = 0
        while c < 10 and proxy.aid in arbiter.managed_actors:
            c += 1
            yield pulsar.NOT_DONE
            arbiter.manage_actors()
        self.assertEqual(proxy.stopping_loops, ACTOR_STOPPING_LOOPS)
        thread_actors = pulsar.process_local_data('thread_actors')
        self.assertFalse(proxy.aid in thread_actors)
        
    @run_on_arbiter
    def testFakeSignal(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        # Now put the signal in the queue
        arbiter.signal_queue.put('foooooo')
        self.assertTrue(arbiter.signal_queue.qsize() >= 1)
        # we need to yield so that the arbiter has a chance to process the signal
        yield pulsar.NOT_DONE
        # The arbiter should have processed the fake signal
        #TODO this is not valid in multiprocessing!
        #self.assertEqual(arbiter.signal_queue.qsize(), 0)
        
    @run_on_arbiter
    def testSignal(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        for sig in system.SIG_NAMES:
            if sig not in EXIT_SIGNALS:
                break
        # send the signal
        arbiter.signal_queue.put(sig)
        self.assertTrue(arbiter.signal_queue.qsize() >= 1)
        yield pulsar.NOT_DONE
        #TODO this is not valid in multiprocessing!
        #self.assertEqual(arbiter.signal_queue.qsize(), 0)
        

@dont_run_with_thread
class TestArbiterProcess(TestArbiterThread):
    impl = 'process'  