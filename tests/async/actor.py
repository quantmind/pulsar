'''Tests actor and actor proxies.'''
from functools import partial
from multiprocessing.queues import Queue

import pulsar
from pulsar import send, get_actor, CommandNotFound
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, ActorTestMixin, run_on_arbiter,\
                                 dont_run_with_thread

from examples.echo.manage import Echo, EchoServerProtocol

def check_actor(actor, name):
    # put something on a queue, just for coverage.
    actor.put(None)
    assert(actor.name==name)
    

def create_echo_server(address, actor):
    sock = pulsar.create_socket(address, bindto=True)
    actor.servers['echo'] = actor.event_loop.create_server(
                                sock=sock,
                                consumer_factory=EchoServerProtocol)
    
    
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
    echo_port = 9898
    
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

    @run_on_arbiter
    def testSimpleSpawn(self):
        '''Test start and stop for a standard actor on the arbiter domain.'''
        proxy = yield self.spawn()
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertEqual(proxy_monitor, proxy)
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(send(proxy.proxy, 'echo', 'Hello!'), 'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        yield self.stop_actors(proxy)
        # lets join the
        proxy_monitor.join(0.5)
        self.assertFalse(proxy_monitor.is_alive())
        
    def test_start_hook(self):
        address = ('localhost', self.echo_port)
        proxy = yield self.spawn(start=partial(create_echo_server, address))
        client = Echo(address)
        result = yield client.request(b'Hello')
        self.assertEqual(result, b'Hello')
        yield self.stop_actors(proxy)

@dont_run_with_thread
class TestActorProcess(TestActorThread):
    concurrency = 'process'
    echo_port = 9899        

