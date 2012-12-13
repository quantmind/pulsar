'''Tests actor and actor proxies.'''
from time import sleep
from multiprocessing.queues import Queue

import pulsar
from pulsar import send
from pulsar.async.defer import pickle
from pulsar.apps.test import unittest, ActorTestMixin, run_on_arbiter,\
                                 dont_run_with_thread


def sleepfunc():
    sleep(2)
    
def on_event(fd, request):
    pass
        
def on_task(self):
    # put something on a queue, just for coverage.
    self.put(None)
    
def check_actor(actor):
    assert(actor.on_task()==None)
    

class DodgyActor(pulsar.Actor):
    
    def on_stop(self):
        raise ValueError()
    
    
class TestProxy(unittest.TestCase):
    
    def test_get_proxy(self):
        self.assertRaises(ValueError, pulsar.get_proxy, 'shcbjsbcjcdcd')
        self.assertEqual(pulsar.get_proxy('shcbjsbcjcdcd', safe=True), None)
    
    def test_dummy_proxy(self):
        actor = pulsar.get_actor()
        self.assertRaises(ValueError, pulsar.concurrency, 'bla',
                          pulsar.Actor, actor, set(), pulsar.Config())
        impl = pulsar.concurrency('thread', pulsar.Actor, actor, set(),
                                  pulsar.Config())
        p = pulsar.ActorProxy(impl)
        self.assertEqual(p.address, None)
        self.assertEqual(p.receive_from(None, 'dummy'), None)
        self.assertEqual(str(p), 'actor(%s)' % p.aid)
        
    def testActorCoverage(self):
        '''test case for coverage'''
        actor = pulsar.get_actor()
        self.assertRaises(ValueError, send, 'sjdcbhjscbhjdbjsj', 'bla')
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
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(send(proxy, 'echo', 'Hello!'), 'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        yield self.stop_actors(self.a)
        # lets join the
        proxy_monitor.join(0.5)
        self.assertFalse(proxy_monitor.is_alive())
        
    def testActorSpawn(self):
        '''Test spawning from actor domain.'''
        yield self.spawn(on_task=on_task, name='pippo')
        proxy = self.a
        self.assertEqual(proxy.name, 'pippo')
        # The current actor is linked with the actor just spawned
        actor = pulsar.get_actor()
        self.assertEqual(actor.get_actor(proxy.aid), proxy)
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(send(proxy, 'echo', 'Hello!'), 'Hello!')
        yield send(proxy, 'run', check_actor)
        
    def testPasswordProtected(self):
        yield self.spawn(password='bla', name='pluto')
        proxy = self.a
        self.assertEqual(proxy.name, 'pluto')
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertRaises(pulsar.AuthenticationError,
                                      send(proxy, 'shutdown'))
        yield self.async.assertEqual(send(proxy, 'auth', 'bla'), True)
        
    def testInfo(self):
        yield self.spawn(on_task=on_task, name='pippo')
        proxy = self.a
        self.assertEqual(proxy.name, 'pippo')
        outcome = send(proxy, 'info')
        yield outcome
        info = outcome.result
        self.assertTrue('actor' in info)
        ainfo = info['actor']
        self.assertEqual(ainfo['is_process'], self.concurrency=='process')
        
    @run_on_arbiter
    def testDodgyActor(self):
        queue = Queue()
        yield self.spawn(actor_class=DodgyActor, max_requests=1,
                         ioqueue=queue, on_event=on_event)
        proxy = pulsar.get_actor().get_actor(self.a.aid)
        self.assertEqual(proxy.name, 'dodgyactor')
        queue.put(('request', 'Hello'))
        c = 0
        while c < 20:
            if not proxy.is_alive():
                break
            else:
                c += 1
                yield pulsar.NOT_DONE
        self.assertFalse(proxy.is_alive())
        
    @run_on_arbiter
    def testHaltServer(self):
        def on_task(w):
            if w.params.done:
                raise pulsar.HaltServer()
            else:
                w.params.done = True
                raise ValueError()
        yield self.spawn(on_task=on_task)
        proxy = pulsar.get_actor().get_actor(self.a.aid)
        c = 0
        while c < 20:
            if not proxy.is_alive():
                break
            else:
                c += 1
                yield pulsar.NOT_DONE
        self.assertFalse(proxy.is_alive())
        

@dont_run_with_thread
class TestActorProcess(TestActorThread):
    impl = 'process'        

