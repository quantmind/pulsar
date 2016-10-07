import pulsar
from pulsar.apps.test import ActorTestMixin
from pulsar import send, async_while

from tests.async import (add, get_test, spawn_actor_from_actor, close_mailbox,
                         wait_for_stop, check_environ)


class ActorTest(ActorTestMixin):

    async def test_spawn_and_interact(self):
        name = 'pluto-%s' % self.concurrency
        proxy = await self.spawn_actor(name=name)
        self.assertEqual(proxy.name, name)
        self.assertEqual(await send(proxy, 'ping'), 'pong')
        self.assertEqual(await send(proxy, 'echo', 'Hello!'), 'Hello!')
        n, result = await send(proxy, 'run', add, 1, 3)
        self.assertEqual(n, name)
        self.assertEqual(result, 4)

    async def test_info(self):
        name = 'pippo-%s' % self.concurrency
        proxy = await self.spawn_actor(name=name)
        self.assertEqual(proxy.name, name)
        info = await send(proxy, 'info')
        self.assertTrue('actor' in info)
        ainfo = info['actor']
        self.assertEqual(ainfo['is_process'],
                         self.concurrency in ('process', 'multi'))

    async def test_simple_spawn(self):
        '''Test start and stop for a standard actor on the arbiter domain.'''
        proxy = await self.spawn_actor(
            name='simple-actor-on-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertEqual(proxy_monitor, proxy)
        self.assertEqual(await send(proxy, 'ping'), 'pong')
        self.assertEqual(await send(proxy.proxy, 'echo', 'Hello!'), 'Hello!')
        # We call the ActorTestMixin.stop_actors method here, since the
        # ActorTestMixin.tearDown method is invoked on the test-worker domain
        # (here we are in the arbiter domain)
        await self.stop_actors(proxy)
        is_alive = await async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)

    async def test_spawn_from_actor(self):
        proxy = await self.spawn_actor(
            name='spawning-actor-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        self.assertTrue(repr(proxy).startswith('spawning-actor-'))
        self.assertEqual(proxy, proxy.proxy)
        proxy_monitor = arbiter.get_actor(proxy.aid)
        self.assertFalse(proxy != proxy_monitor)
        #
        # do the spawning
        name = 'spawned-actor-%s-from-actor' % self.concurrency
        aid = await send(proxy, 'run', spawn_actor_from_actor, name)
        self.assertTrue(aid)
        proxy_monitor2 = arbiter.get_actor(aid)
        self.assertEqual(proxy_monitor2.name, name)
        self.assertNotEquals(proxy_monitor, proxy_monitor2)
        #
        # stop them
        await self.stop_actors(proxy, proxy_monitor2)
        is_alive = await async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)
        is_alive = await async_while(3, proxy_monitor2.is_alive)
        self.assertFalse(is_alive)

    async def test_config_command(self):
        proxy = await self.spawn_actor(
            name='actor-test-config-%s' % self.concurrency)
        arbiter = pulsar.get_actor()
        proxy_monitor = arbiter.get_actor(proxy.aid)
        result = await send(proxy, 'config', 'khjkh', 'name')
        self.assertEqual(result, None)
        result = await send(proxy, 'config', 'get', 'concurrency')
        self.assertEqual(result, self.concurrency)
        result = await send(proxy, 'config', 'get', 'concurrency', 'foo')
        self.assertEqual(result, None)
        #
        result = await send(proxy, 'config', 'set', 'max_requests', 1000, 1000)
        self.assertEqual(result, None)
        result = await send(proxy, 'config', 'set', 'max_requests', 1000)
        self.assertEqual(result, True)
        result = await send(proxy, 'config', 'get', 'max_requests')
        self.assertEqual(result, 1000)
        #
        await self.stop_actors(proxy)
        is_alive = await async_while(3, proxy_monitor.is_alive)
        self.assertFalse(is_alive)

    async def test_get_application(self):
        proxy = await self.spawn_actor(
            name='actor-test-get-application-%s' % self.concurrency)
        cfg = await send(proxy, 'run', get_test)
        self.assertTrue(cfg)
        self.assertEqual(cfg.name, 'test')

    async def test_connection_lost(self):
        proxy = await self.spawn_actor(
            name='actor-test-connection-lost-%s' % self.concurrency)
        await send(proxy, 'run', close_mailbox)
        await wait_for_stop(self, proxy.aid, True)

    async def test_environment(self):
        import os
        os.environ['PULSAR_TEST_ENVIRON'] = 'yes'
        name = 'environment-%s' % self.concurrency
        proxy = await self.spawn_actor(name=name)
        value = await send(proxy, 'run', check_environ, 'PULSAR_TEST_ENVIRON')
        self.assertEqual(value, 'yes')
