'''Tests for arbiter and monitors.'''
import os

import pulsar
from pulsar import (send, spawn, system, platform, ACTOR_ACTION_TIMEOUT,
                    MONITOR_TASK_PERIOD, multi_async, Deferred)
from pulsar.utils.pep import default_timer
from pulsar.apps.test import (unittest, run_on_arbiter, ActorTestMixin,
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
        actor.stop = lambda exc=None: False
    else:
        actor.event_loop.call_soon(cause_timeout, actor)


class TestArbiterThread(ActorTestMixin, unittest.TestCase):
    concurrency = 'thread'

    @run_on_arbiter
    def testArbiterObject(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter, pulsar.arbiter())
        self.assertTrue(arbiter.is_arbiter())
        self.assertEqual(arbiter.impl.kind, 'arbiter')
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
        yield future
        proxy = future.result
        self.assertEqual(future.aid, proxy.aid)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        yield send(proxy, 'stop')

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
        proxy = yield self.spawn(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        yield send(proxy, 'run', cause_timeout)
        # The arbiter should soon start to stop the actor
        interval = 2*MONITOR_TASK_PERIOD
        yield pulsar.async_while(interval,
                                 lambda: not proxy.stopping_start)
        self.assertTrue(proxy.stopping_start)
        #
        yield pulsar.async_while(interval,
                                 lambda: proxy.aid in arbiter.managed_actors)
        self.assertFalse(proxy.aid in arbiter.managed_actors)

    @run_on_arbiter
    def test_terminate(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'bogus-term-%s' % self.concurrency
        proxy = yield self.spawn(name=name, timeout=1)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        #
        result = yield send(proxy, 'run', cause_terminate)
        #
        # The arbiter should soon start stop the actor
        interval = 3*MONITOR_TASK_PERIOD
        yield pulsar.async_while(2*MONITOR_TASK_PERIOD,
                                 lambda: not proxy.stopping_start)
        self.assertTrue(proxy.stopping_start)
        #
        yield pulsar.async_while(1.5*ACTOR_ACTION_TIMEOUT,
                                 lambda: proxy.aid in arbiter.managed_actors)
        self.assertTrue(proxy in arbiter.terminated_actors)
        self.assertFalse(proxy.aid in arbiter.managed_actors)

    @run_on_arbiter
    def test_actor_termination(self):
        '''Terminate the remote actor via the concurreny terminate method.'''
        arbiter = pulsar.get_actor()
        self.assertTrue(arbiter.is_arbiter())
        name = 'actor-term-%s' % self.concurrency
        proxy = yield self.spawn(name=name)
        self.assertEqual(proxy.name, name)
        self.assertTrue(proxy.aid in arbiter.managed_actors)
        proxy = arbiter.managed_actors[proxy.aid]
        #
        # terminate the actor and see what appens
        proxy.terminate()
        #
        # The arbiter should soon start stop the actor
        interval = 3*MONITOR_TASK_PERIOD
        #
        yield pulsar.async_while(interval,
                                 lambda: proxy.aid in arbiter.managed_actors)
        self.assertFalse(proxy in arbiter.terminated_actors)
        self.assertFalse(proxy.aid in arbiter.managed_actors)

    def test_no_arbiter_in_worker_domain(self):
        worker = pulsar.get_actor()
        self.assertFalse(worker.is_arbiter())
        self.assertEqual(pulsar.arbiter(), None)
        self.assertTrue(worker.monitor)
        self.assertNotEqual(worker.monitor.name, 'arbiter')


@dont_run_with_thread
class TestArbiterProcess(TestArbiterThread):
    concurrency = 'process'
