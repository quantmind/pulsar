'''Tests actor and actor proxies.'''
import unittest

from multiprocessing.queues import Queue
from functools import partial

import pulsar
from pulsar import (send, get_actor, CommandNotFound, async_while, TcpServer,
                    coroutine_return, Connection)
from pulsar.utils.pep import pickle, default_timer
from pulsar.apps.test import (ActorTestMixin, run_on_arbiter,
                              dont_run_with_thread, mute_failure)

from examples.echo.manage import Echo, EchoServerProtocol


def add(actor, a, b):
    return (actor.name, a+b)


class create_echo_server(object):
    '''partial is not picklable in python 2.6'''
    def __init__(self, address):
        self.address = address

    def __call__(self, actor):
        '''Starts an echo server on a newly spawn actor'''
        address = self.address
        server = TcpServer(partial(Connection, EchoServerProtocol),
                           actor._loop, self.address)
        yield server.start_serving()
        actor.servers['echo'] = server
        actor.extra['echo-address'] = server.address
        actor.bind_event('stopping', self._stop_server)
        coroutine_return(actor)

    def _stop_server(self, actor):
        yield actor.servers['echo'].close()
        coroutine_return(actor)


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

    def test_actor_coverage(self):
        '''test case for coverage'''
        actor = pulsar.get_actor()
        try:
            yield send(send, 'sjdcbhjscbhjdbjsj', 'bla')
        except CommandNotFound:
            pass
        #self.assertRaises(pickle.PicklingError, pickle.dumps, actor)


class TestActorThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'

    def test_spawn_and_interact(self):
        proxy = yield self.spawn_actor(name='pluto')
        self.assertEqual(proxy.name, 'pluto')
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(send(proxy, 'echo', 'Hello!'), 'Hello!')
        name, result = yield send(proxy, 'run', add, 1, 3)
        self.assertEqual(name, 'pluto')
        self.assertEqual(result, 4)

    def test_info(self):
        proxy = yield self.spawn_actor(name='pippo')
        self.assertEqual(proxy.name, 'pippo')
        info = yield send(proxy, 'info')
        self.assertTrue('actor' in info)
        ainfo = info['actor']
        self.assertEqual(ainfo['is_process'], self.concurrency == 'process')

    @run_on_arbiter
    def testSimpleSpawn(self):
        '''Test start and stop for a standard actor on the arbiter domain.'''
        proxy = yield self.spawn_actor(
            name='simple-actor-on-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertEqual(proxy_monitor, proxy)
        yield self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield self.async.assertEqual(send(proxy.proxy, 'echo', 'Hello!'),
                                     'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        yield self.stop_actors(proxy)
        is_alive = yield async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)

class d:
    def test_start_hook(self):
        proxy = yield self.spawn_actor(
            start=create_echo_server(('127.0.0.1', 0)))
        address = None
        start = default_timer()
        while not address:
            info = yield send(proxy, 'info')
            address = info['extra'].get('echo-address')
            if default_timer() - start > 3:
                break
        self.assertTrue(address)
        echo = Echo(address)
        result = yield echo(b'Hello')
        self.assertEqual(result, b'Hello')
        yield self.stop_actors(proxy)


@dont_run_with_thread
class TestActorProcess(TestActorThread):
    concurrency = 'process'
