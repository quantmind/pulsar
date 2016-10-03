'''Tests for arbiter and monitors.'''
import unittest
import asyncio

import pulsar
from pulsar import send, spawn, ACTOR_ACTION_TIMEOUT
from pulsar.apps.test import ActorTestMixin, test_timeout

from tests.async import cause_timeout, cause_terminate, wait_for_stop


class TestArbiterProcess(ActorTestMixin, unittest.TestCase):
    concurrency = 'process'

    def test_arbiter_object(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter, pulsar.arbiter())
        self.assertTrue(arbiter.is_arbiter())
        self.assertEqual(arbiter.impl.kind, 'arbiter')
        self.assertEqual(arbiter.aid, 'arbiter')
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertTrue(arbiter.monitors)
        self.assertEqual(arbiter.exit_code, None)
        info = arbiter.info()
        self.assertTrue('server' in info)
        server = info['server']
        self.assertEqual(server['state'], 'running')

    def test_arbiter_mailbox(self):
        arbiter = pulsar.get_actor()
        mailbox = arbiter.mailbox
        self.assertFalse(hasattr(mailbox, 'request'))
        # Same for all monitors mailboxes
        for monitor in arbiter.monitors.values():
            mailbox = monitor.mailbox
            self.assertFalse(hasattr(mailbox, 'request'))

    def test_registered(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        self.assertTrue(arbiter.registered)
        self.assertTrue('arbiter' in arbiter.registered)
        self.assertTrue('test' in arbiter.registered)

    @test_timeout(2*ACTOR_ACTION_TIMEOUT)
    async def test_spawning_in_arbiter(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertTrue(len(arbiter.monitors) >= 1)
        name = 'testSpawning-%s' % self.concurrency
        future = spawn(name=name, concurrency=self.concurrency)
        self.assertTrue(future.aid in arbiter.managed_actors)
        proxy = await future
        self.assertEqual(future.aid, proxy.aid)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        self.assertEqual(proxy, arbiter.get_actor(proxy.aid))
        #
        await asyncio.sleep(1)
        # Stop the actor
        result = await send(proxy, 'stop')
        self.assertEqual(result, None)
        #
        result = await wait_for_stop(self, proxy.aid)
        self.assertEqual(result, None)

    def test_bad_monitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.monitors)
        name = list(arbiter.monitors.values())[0].name
        self.assertRaises(KeyError, arbiter.add_monitor, name)

    @test_timeout(2*ACTOR_ACTION_TIMEOUT)
    async def test_terminate(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-term-%s' % self.concurrency
        proxy = await self.spawn_actor(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        #
        await send(proxy, 'run', cause_terminate)
        #
        # The arbiter should now terminate the actor
        await wait_for_stop(self, proxy.aid, True)
        #
        self.assertTrue(proxy.stopping_start)

    async def test_actor_timeout(self):
        """Test a bogus actor for timeout"""
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-timeout-%s' % self.concurrency
        proxy = await self.spawn_actor(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        await send(proxy, 'run', cause_timeout)
        # The arbiter should soon start to stop the actor
        await wait_for_stop(self, proxy.aid, True)
        #
        self.assertTrue(proxy.stopping_start)
        self.assertFalse(proxy.aid in arbiter.managed_actors)
