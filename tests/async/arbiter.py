'''Tests for arbiter and monitors.'''
import os
import unittest

import pulsar
from pulsar import (send, spawn, system, platform, ACTOR_ACTION_TIMEOUT,
                    MONITOR_TASK_PERIOD, multi_async)
from pulsar.utils.pep import default_timer
from pulsar.apps.test import (run_on_arbiter, ActorTestMixin,
                              dont_run_with_thread)


def timeout(start):
    return default_timer() - start > 1.5*ACTOR_ACTION_TIMEOUT


def cause_timeout(actor):
    if actor.next_periodic_task:
        actor.next_periodic_task.cancel()
    else:
        actor.event_loop.call_soon(cause_timeout, actor)


def cause_terminate(actor):
    if actor.next_periodic_task:
        actor.next_periodic_task.cancel()
        # hayjack the stop method
        actor.stop = lambda exc=None, exit_code=None: False
    else:
        actor.event_loop.call_soon(cause_timeout, actor)


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

    @run_on_arbiter
    def test_arbiter_object(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter, pulsar.arbiter())
        self.assertTrue(arbiter.is_arbiter())
        self.assertEqual(arbiter.impl.kind, 'arbiter')
        self.assertEqual(arbiter.identity, 'arbiter')
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertNotEqual(arbiter.identity, arbiter.aid)
        self.assertTrue(arbiter.monitors)
        self.assertEqual(arbiter.exit_code, None)
        info = arbiter.info()
        self.assertTrue('server' in info)
        server = info['server']
        self.assertEqual(server['state'], 'running')

    @run_on_arbiter
    def test_arbiter_mailbox(self):
        arbiter = pulsar.get_actor()
        mailbox = arbiter.mailbox
        self.assertFalse(hasattr(mailbox, 'request'))
        # Same for all monitors mailboxes
        for monitor in arbiter.monitors.values():
            mailbox = monitor.mailbox
            self.assertFalse(hasattr(mailbox, 'request'))

    @run_on_arbiter
    def test_registered(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        self.assertTrue(arbiter.registered)
        self.assertTrue('arbiter' in arbiter.registered)
        self.assertTrue('test' in arbiter.registered)

    @run_on_arbiter
    def test_ping_test_worker(self):
        arbiter = pulsar.get_actor()
        info = arbiter.info()
        test = info['monitors']['test']
        workers = [w['actor']['actor_id'] for w in test['workers']]
        self.assertTrue(workers)
        result = yield multi_async((arbiter.send(w, 'ping') for w in workers))
        self.assertEqual(len(result), len(workers))
        self.assertEqual(result, len(result)*['pong'])

    @run_on_arbiter
    def test_spawning_in_arbiter(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter.name, 'arbiter')
        self.assertTrue(len(arbiter.monitors) >= 1)
        name = 'testSpawning-%s' % self.concurrency
        future = spawn(name=name, concurrency=self.concurrency)
        self.assertTrue(future.aid in arbiter.managed_actors)
        proxy = yield future
        self.assertEqual(future.aid, proxy.aid)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        self.assertEqual(proxy, arbiter.get_actor(proxy.aid))
        #
        # Stop the actor
        result = yield send(proxy, 'stop')
        self.assertEqual(result, None)
        #
        result = yield wait_for_stop(self, proxy.aid)
        self.assertEqual(result, None)

    @run_on_arbiter
    def testBadMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.monitors)
        name = list(arbiter.monitors.values())[0].name
        self.assertRaises(KeyError, arbiter.add_monitor, name)

    @run_on_arbiter
    def testTimeout(self):
        '''Test a bogus actor for timeout.'''
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-timeout-%s' % self.concurrency
        proxy = yield self.spawn_actor(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        yield send(proxy, 'run', cause_timeout)
        # The arbiter should soon start to stop the actor
        yield wait_for_stop(self, proxy.aid, True)
        #
        self.assertTrue(proxy.stopping_start)
        self.assertFalse(proxy.aid in arbiter.managed_actors)

    @run_on_arbiter
    def test_terminate(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-term-%s' % self.concurrency
        proxy = yield self.spawn_actor(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        #
        result = yield send(proxy, 'run', cause_terminate)
        #
        # The arbiter should soon start stop the actor
        yield wait_for_stop(self, proxy.aid, True)
        #
        self.assertTrue(proxy.stopping_start)
        self.assertTrue(proxy in arbiter.terminated_actors)

    def test_no_arbiter_in_worker_domain(self):
        worker = pulsar.get_actor()
        self.assertFalse(worker.is_arbiter())
        self.assertEqual(pulsar.arbiter(), None)
        self.assertTrue(worker.monitor)
        self.assertNotEqual(worker.monitor.name, 'arbiter')


@dont_run_with_thread
class TestArbiterProcess(TestArbiterThread):
    concurrency = 'process'
