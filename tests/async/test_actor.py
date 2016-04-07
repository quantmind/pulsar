'''Tests actor and actor proxies.'''
import unittest
import asyncio

from functools import partial

import pulsar
from pulsar import send, async_while, TcpServer, Connection
from pulsar.apps.test import ActorTestMixin, dont_run_with_thread

from examples.echo.manage import EchoServerProtocol
from tests.async import add, get_test, spawn_actor_from_actor


class create_echo_server:
    '''partial is not picklable in python 2.6'''
    def __init__(self, address):
        self.address = address

    @asyncio.coroutine
    def __call__(self, actor):
        '''Starts an echo server on a newly spawn actor'''
        server = TcpServer(partial(Connection, EchoServerProtocol),
                           actor._loop, self.address)
        yield from server.start_serving()
        actor.servers['echo'] = server
        actor.extra['echo-address'] = server.address
        actor.bind_event('stopping', self._stop_server)
        return actor

    @asyncio.coroutine
    def _stop_server(self, actor):
        yield from actor.servers['echo'].close()
        return actor


class TestActorThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'

    @asyncio.coroutine
    def test_spawn_and_interact(self):
        name = 'pluto-%s' % self.concurrency
        proxy = yield from self.spawn_actor(name=name)
        self.assertEqual(proxy.name, name)
        yield from self.wait.assertEqual(send(proxy, 'ping'), 'pong')
        yield from self.wait.assertEqual(send(proxy, 'echo', 'Hello!'),
                                         'Hello!')
        n, result = yield from send(proxy, 'run', add, 1, 3)
        self.assertEqual(n, name)
        self.assertEqual(result, 4)

    @asyncio.coroutine
    def test_info(self):
        name = 'pippo-%s' % self.concurrency
        proxy = yield from self.spawn_actor(name=name)
        self.assertEqual(proxy.name, name)
        info = yield from send(proxy, 'info')
        self.assertTrue('actor' in info)
        ainfo = info['actor']
        self.assertEqual(ainfo['is_process'], self.concurrency == 'process')

    @asyncio.coroutine
    def test_simple_spawn(self):
        '''Test start and stop for a standard actor on the arbiter domain.'''
        proxy = yield from self.spawn_actor(
            name='simple-actor-on-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertEqual(proxy_monitor, proxy)
        yield from self.wait.assertEqual(send(proxy, 'ping'), 'pong')
        yield from self.wait.assertEqual(send(proxy.proxy, 'echo', 'Hello!'),
                                         'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        yield from self.stop_actors(proxy)
        is_alive = yield from async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)

    @asyncio.coroutine
    def test_spawn_from_actor(self):
        proxy = yield from self.spawn_actor(
            name='spawning-actor-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        self.assertTrue(repr(proxy).startswith('spawning-actor-'))
        self.assertEqual(proxy, proxy.proxy)
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertFalse(proxy != proxy_monitor)
        #
        # do the spawning
        name = 'spawned-actor-%s-from-actor' % self.concurrency
        aid = yield from send(proxy, 'run', spawn_actor_from_actor, name)
        self.assertTrue(aid)
        proxy_monitor2 = arbiter.get_actor(aid)
        self.assertEqual(proxy_monitor2.name, name)
        self.assertNotEquals(proxy_monitor, proxy_monitor2)
        #
        # stop them
        yield from self.stop_actors(proxy, proxy_monitor2)
        is_alive = yield from async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)
        is_alive = yield from async_while(3, proxy_monitor2.is_alive)
        self.assertFalse(is_alive)

    @asyncio.coroutine
    def test_config_command(self):
        proxy = yield from self.spawn_actor(
            name='actor-test-config-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        result = yield from send(proxy, 'config', 'khjkh', 'name')
        self.assertEqual(result, None)
        result = yield from send(proxy, 'config', 'get', 'concurrency')
        self.assertEqual(result, self.concurrency)
        result = yield from send(proxy, 'config', 'get', 'concurrency', 'foo')
        self.assertEqual(result, None)
        #
        result = yield from send(proxy, 'config', 'set', 'max_requests',
                                 1000, 1000)
        self.assertEqual(result, None)
        result = yield from send(proxy, 'config', 'set', 'max_requests',
                                 1000)
        self.assertEqual(result, True)
        result = yield from send(proxy, 'config', 'get', 'max_requests')
        self.assertEqual(result, 1000)
        #
        yield from self.stop_actors(proxy)
        is_alive = yield from async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)

    @asyncio.coroutine
    def test_get_application(self):
        proxy = yield from self.spawn_actor(
            name='actor-test-get-application-%s' % self.concurrency)
        cfg = yield from send(proxy, 'run', get_test)
        self.assertTrue(cfg)
        self.assertEqual(cfg.name, 'test')


@dont_run_with_thread
class TestActorProcess(TestActorThread):
    concurrency = 'process'
