'''Tests for arbiter and monitors.'''
import unittest
import asyncio

import pulsar
from pulsar import send, spawn, ACTOR_ACTION_TIMEOUT
from pulsar.apps.test import (ActorTestMixin, dont_run_with_thread,
                              test_timeout)

from tests.async import cause_timeout, cause_terminate


def wait_for_stop(test, aid, terminating=False):
    '''Wait for an actor to stop'''
    arbiter = pulsar.arbiter()
    waiter = pulsar.Future(loop=arbiter._loop)

    def remove():
        test.assertEqual(arbiter.remove_callback('periodic_task', check), 1)
        waiter.set_result(None)

    def check(caller, **kw):
        test.assertEqual(caller, arbiter)
        if not terminating:
            test.assertFalse(aid in arbiter.managed_actors)
        elif aid in arbiter.managed_actors:
            return
        arbiter._loop.call_soon(remove)

    arbiter.bind_event('periodic_task', check)
    return waiter


class TestArbiterThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'

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
    @asyncio.coroutine
    def test_spawning_in_arbiter(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertTrue(len(arbiter.monitors) >= 1)
        name = 'testSpawning-%s' % self.concurrency
        future = spawn(name=name, concurrency=self.concurrency)
        self.assertTrue(future.aid in arbiter.managed_actors)
        proxy = yield from future
        self.assertEqual(future.aid, proxy.aid)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        self.assertEqual(proxy, arbiter.get_actor(proxy.aid))
        #
        yield from asyncio.sleep(1)
        # Stop the actor
        result = yield from send(proxy, 'stop')
        self.assertEqual(result, None)
        #
        result = yield from wait_for_stop(self, proxy.aid)
        self.assertEqual(result, None)

    def testBadMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.monitors)
        name = list(arbiter.monitors.values())[0].name
        self.assertRaises(KeyError, arbiter.add_monitor, name)

    @asyncio.coroutine
    def testTimeout(self):
        '''Test a bogus actor for timeout.'''
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-timeout-%s' % self.concurrency
        proxy = yield from self.spawn_actor(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        yield from send(proxy, 'run', cause_timeout)
        # The arbiter should soon start to stop the actor
        yield from wait_for_stop(self, proxy.aid, True)
        #
        self.assertTrue(proxy.stopping_start)
        self.assertFalse(proxy.aid in arbiter.managed_actors)

    @test_timeout(2*ACTOR_ACTION_TIMEOUT)
    @asyncio.coroutine
    def test_terminate(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-term-%s' % self.concurrency
        proxy = yield from self.spawn_actor(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        #
        yield from send(proxy, 'run', cause_terminate)
        #
        # The arbiter should now terminate the actor
        yield from wait_for_stop(self, proxy.aid, True)
        #
        self.assertTrue(proxy.stopping_start)
        self.assertTrue(proxy in arbiter.terminated_actors)


@dont_run_with_thread
class TestArbiterProcess(TestArbiterThread):
    concurrency = 'process'
