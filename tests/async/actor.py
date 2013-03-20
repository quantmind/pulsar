'''Tests actor and actor proxies.'''
from time import sleep
from multiprocessing.queues import Queue

import pulsar
from pulsar import send, get_actor, CommandNotFound
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, ActorTestMixin, run_on_arbiter,\
                                 dont_run_with_thread


def sleepfunc():
    sleep(2)
    
def on_event(fd, request):
    pass
        
def check_actor(actor, name):
    # put something on a queue, just for coverage.
    actor.put(None)
    assert(actor.name==name)
    

class DodgyActor(pulsar.Actor):
    
    def on_stop(self):
        raise ValueError()
    
    
def halt_actor(halt=0):
    actor = get_actor()
    if halt==2:
        actor.requestloop.stop()
    elif halt:
        actor.requestloop.call_soon(halt_actor, 2)
        raise ValueError()
    else:   # called at the beginning
        actor.requestloop.call_soon(halt_actor, 1)
    
    
class TestProxy(unittest.TestCase):
    
    def test_get_proxy(self):
        self.assertRaises(ValueError, pulsar.get_proxy, 'shcbjsbcjcdcd')
        self.assertEqual(pulsar.get_proxy('shcbjsbcjcdcd', safe=True), None)
    
    def test_bad_concurrency(self):
        actor = pulsar.get_actor()
        # bla concurrency does not exists
        self.assertRaises(ValueError, pulsar.concurrency, 'bla', pulsar.Actor,
                          actor, pulsar.Config())
    
    def test_dummy_proxy(self):
        p = pulsar.concurrency('thread', pulsar.Actor, pulsar.get_actor(),
                               pulsar.Config())
        self.assertEqual(p.mailbox, None)
        self.assertEqual(p.spawning_start, None)
        self.assertEqual(p.stopping_start, None)
        self.assertEqual(p.callback, None)
        self.assertEqual(str(p), 'actor(%s)' % p.aid)
        
    def testActorCoverage(self):
        '''test case for coverage'''
        actor = pulsar.get_actor()
        self.assertRaises(CommandNotFound, send, 'sjdcbhjscbhjdbjsj', 'bla')
        self.assertRaises(pickle.PicklingError, pickle.dumps, actor)
        

class TestActorThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'
    
    def test_spawn_actor(self):
        '''Test spawning from actor domain.'''
        proxy = yield self.spawn(name='pippo')
        yield self.assertEqual(proxy.name, 'pippo')
        # The current actor is linked with the actor just spawned
        
    def test_spawn_and_interact(self):
        proxy = yield self.spawn(name='pluto')
        self.assertEqual(proxy.name, 'pluto')
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(send(proxy, 'echo', 'Hello!'), 'Hello!')
        #yield send(proxy, 'run', check_actor, 'pluto')
        
    def test_info(self):
        proxy = yield self.spawn(name='pippo')
        self.assertEqual(proxy.name, 'pippo')
        info = yield send(proxy, 'info')
        self.assertTrue('actor' in info)
        ainfo = info['actor']
        self.assertEqual(ainfo['is_process'], self.concurrency=='process')
      
        
class a:

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
        yield self.spawn(name='halting_actor', on_start=halt_actor)
        proxy = pulsar.get_actor().get_actor(self.a.aid)
        c = 0
        while c < 20:
            if not proxy.is_alive():
                break
            else:
                c += 1
                yield pulsar.NOT_DONE
        self.assertFalse(proxy.is_alive())
        

#@dont_run_with_thread
#class TestActorProcess(TestActorThread):
#    impl = 'process'        

