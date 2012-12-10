'''Tests actor and actor proxies.'''
from time import sleep

import pulsar
from pulsar.async.defer import pickle
from pulsar.apps.test import unittest, ActorTestMixin, run_on_arbiter,\
                                 dont_run_with_thread


def sleepfunc():
    sleep(2)
    
def on_task(self):
    # put something on a queue, just for coverage.
    self.put(None)
    
def check_actor(actor):
    assert(actor.on_task()==None)
    
    
class TestProxy(unittest.TestCase):
    
    def test_get_proxy(self):
        self.assertRaises(ValueError, pulsar.get_proxy, 'shcbjsbcjcdcd')
        self.assertEqual(pulsar.get_proxy('shcbjsbcjcdcd', safe=True), None)
    
    def test_dummy_proxy(self):
        actor = pulsar.get_actor()
        self.assertRaises(ValueError, pulsar.concurrency, 'bla',
                          pulsar.Actor, actor, set(), pulsar.Config(), {})
        impl = pulsar.concurrency('thread', pulsar.Actor, actor, set(),
                                  pulsar.Config(), {})
        p = pulsar.ActorProxy(impl)
        self.assertEqual(p.address, None)
        self.assertEqual(p.receive_from(None, 'dummy'), None)
        self.assertEqual(str(p), p.aid)
        
    def testActorCoverage(self):
        '''test case for coverage'''
        actor = pulsar.get_actor()
        self.assertRaises(ValueError, actor.send, 'sjdcbhjscbhjdbjsj', 'bla')
        self.assertRaises(pickle.PicklingError, pickle.dumps, actor)
        
    
class TestActorThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'
    
    @run_on_arbiter
    def testSimpleSpawn(self):
        '''Test start and stop for a standard actor on the arbiter domain.'''
        yield self.spawn()
        proxy = self.a
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertEqual(proxy_monitor.aid, proxy.aid)
        self.assertEqual(proxy_monitor.address, proxy.address)
        yield self.async.assertEqual(arbiter.send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(arbiter.send(proxy, 'echo', 'Hello!'),
                                     'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        yield self.stop_actors(self.a)
        # lets join the
        proxy_monitor.join(0.5)
        self.assertFalse(proxy_monitor.is_alive())
        
    def testActorSpawn(self):
        '''Test spawning from actor domain.'''
        yield self.spawn(on_task=on_task, concurrency='thread')
        proxy = self.a
        actor = pulsar.get_actor()
        # The current actor is linked with the actor just spawned
        self.assertEqual(actor.get_actor(proxy.aid), proxy)
        yield self.async.assertEqual(actor.send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(actor.send(proxy, 'echo', 'Hello!'),
                                     'Hello!')
        yield actor.send(proxy, 'run', check_actor)
        
    def testPasswordProtected(self):
        cfg = pulsar.Config()
        cfg.set('password', 'bla')
        yield self.spawn(cfg=cfg)
        proxy = self.a
        actor = pulsar.get_actor()
        yield self.async.assertEqual(actor.send(proxy, 'ping'), 'pong')
        yield self.async.assertRaises(pulsar.AuthenticationError,
                                      actor.send(proxy, 'shutdown'))
        yield self.async.assertEqual(actor.send(proxy, 'auth', 'bla'), True)
        
        

@dont_run_with_thread
class TestActorProcess(TestActorThread):
    impl = 'process'        

