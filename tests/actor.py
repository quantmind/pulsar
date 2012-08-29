'''Tests actor and actor proxies.'''
from time import sleep

import pulsar
from pulsar.apps.test import unittest, ActorTestMixin, run_on_arbiter,\
                                 dont_run_with_thread


def sleepfunc():
    sleep(2)
    
def on_task(self):
    # put something on a queue, just for coverage.
    self.put(None)
    
def check_actor(actor):
    assert(actor.on_task()==None)
    
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
        yield self.spawn(cfg={'password': 'bla', 'param': 1})
        proxy = self.a
        actor = pulsar.get_actor()
        yield self.async.assertEqual(actor.send(proxy, 'ping'), 'pong')
        yield self.async.assertRaises(pulsar.AuthenticationError,
                                      actor.send(proxy, 'shutdown'))
        yield self.async.assertEqual(actor.send(proxy, 'auth', 'bla'), True)
        
    def __testStartStopQueue(self):
        '''Test start and stop for an actor using a I/O queue'''
        arbiter = pulsar.arbiter()
        ioqueue = pulsar.Queue()
        yield self.spawn(impl = self.impl, name = 'queue',
                         ioqueue = ioqueue)
        a = self.a
        self.assertTrue(isinstance(a,pulsar.ActorProxy))
        self.assertEqual(a.impl.impl,self.impl)
        outcome = a.send(arbiter, 'ping')
        yield outcome
        self.assertEqual(outcome.result,'pong')
        yield self.stop()
        
    def __testSpawnStopFromActor(self):
        '''Test the global spawn method from an actor domain other than the
arbiter'''
        outcome = pulsar.spawn(impl = self.impl)
        # the result is an deferred message
        self.assertTrue(isinstance(outcome, pulsar.ActorProxyDeferred))
        yield outcome
        ap = outcome.result
        self.assertTrue(isinstance(ap, pulsar.ActorProxy))
        # Check that the new actor is linked with the current actor
        self.assertEqual(ap, self.worker.get_actor(ap.aid))
        # and now stop the new actor
        outcome = pulsar.send(ap, 'stop')
        yield outcome
        #self.assertEqual(self.worker.get_actor(ap.aid),None)
        
    def __testInfo(self):
        a = spawn(Actor, impl = self.impl)
        cbk = self.Callback()
        r = self.arbiter.proxy.info(a).add_callback(cbk)
        self.wait(lambda : not hasattr(cbk,'result'))
        self.assertFalse(r.rid in ActorRequest.REQUESTS)
        info = cbk.result
        self.assertEqual(info['aid'],a.aid)
        self.assertEqual(info['pid'],a.pid)
        self.stop(a)
        
    def __testSpawnFew(self):
        actors = (spawn(Actor, impl = self.impl) for i in range(5))
        for a in actors:
            self.assertTrue(a.aid in self.arbiter.LIVE_ACTORS)
            cbk = self.Callback()
            r = self.arbiter.proxy.ping(a).add_callback(cbk)
            self.wait(lambda : not hasattr(cbk,'result'))
            self.assertEqual(cbk.result,'pong')
            self.assertFalse(r.rid in ActorRequest.REQUESTS)
                
    def __testTimeout(self):
        a = spawn(Actor, on_task = sleepfunc, impl = self.impl, timeout = 1)
        self.assertTrue(a.aid in self.arbiter.LIVE_ACTORS)
        self.wait(lambda : a.aid in self.arbiter.LIVE_ACTORS, timeout = 3)
        self.assertFalse(a.aid in self.arbiter.LIVE_ACTORS)
        

@dont_run_with_thread
class TestActorProcess(TestActorThread):
    impl = 'process'        

