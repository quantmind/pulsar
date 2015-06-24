'''Tests actor and actor proxies.'''
import unittest

from functools import partial

import pulsar
from pulsar import send, async_while, TcpServer, Connection
from pulsar.apps.test import ActorTestMixin, dont_run_with_thread

from examples.echo.manage import EchoServerProtocol


def add(actor, a, b):
    return (actor.name, a+b)


class create_echo_server(object):
    '''partial is not picklable in python 2.6'''
    def __init__(self, address):
        self.address = address

    def __call__(self, actor):
        '''Starts an echo server on a newly spawn actor'''
        server = TcpServer(partial(Connection, EchoServerProtocol),
                           actor._loop, self.address)
        yield from server.start_serving()
        actor.servers['echo'] = server
        actor.extra['echo-address'] = server.address
        actor.bind_event('stopping', self._stop_server)
        return actor

    def _stop_server(self, actor):
        yield from actor.servers['echo'].close()
        return actor


class TestActorThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'

    def test_spawn_and_interact(self):
        name = 'pluto-%s' % self.concurrency
        proxy = yield from self.spawn_actor(name=name)
        self.assertEqual(proxy.name, name)
        yield from self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield from self.async.assertEqual(send(proxy, 'echo', 'Hello!'),
                                          'Hello!')
        n, result = yield from send(proxy, 'run', add, 1, 3)
        self.assertEqual(n, name)
        self.assertEqual(result, 4)

    def test_info(self):
        name = 'pippo-%s' % self.concurrency
        proxy = yield from self.spawn_actor(name=name)
        self.assertEqual(proxy.name, name)
        info = yield from send(proxy, 'info')
        self.assertTrue('actor' in info)
        ainfo = info['actor']
        self.assertEqual(ainfo['is_process'], self.concurrency == 'process')

    def test_simple_spawn(self):
        '''Test start and stop for a standard actor on the arbiter domain.'''
        proxy = yield from self.spawn_actor(
            name='simple-actor-on-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertEqual(proxy_monitor, proxy)
        yield from self.async.assertEqual(send(proxy, 'ping'), 'pong')
        yield from self.async.assertEqual(send(proxy.proxy, 'echo', 'Hello!'),
                                          'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        yield from self.stop_actors(proxy)
        is_alive = yield from async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)


@dont_run_with_thread
class TestActorProcess(TestActorThread):
    concurrency = 'process'
