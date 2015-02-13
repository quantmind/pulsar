'''Tests the test suite and pulsar distribution.'''
import os
import unittest
from threading import current_thread

import pulsar
from pulsar import asyncio, send, multi_async, get_event_loop, Future
from pulsar.apps.test import TestSuite, sequential
from pulsar.apps.test.plugins import bench, profile
from pulsar.utils.version import get_version


def simple_function(actor):
    return actor.name


def wait(actor, period=0.5):
    start = actor._loop.time()
    yield from asyncio.sleep(period)
    return actor._loop.time() - start


class TestTestWorker(unittest.TestCase):

    def test_ping_pong_monitor(self):
        value = yield 3
        self.assertEqual(value, 3)
        try:
            future = Future()
            future.set_exception(ValueError('test'))
            yield future
        except ValueError:
            pass
        pong = yield send('monitor', 'ping')
        self.assertEqual(pong, 'pong')

    def test_worker(self):
        '''Test the test worker'''
        worker = pulsar.get_actor()
        self.assertTrue(pulsar.is_actor(worker))
        self.assertTrue(worker.is_running())
        self.assertFalse(worker.closed())
        self.assertFalse(worker.stopped())
        self.assertEqual(worker.info_state, 'running')
        self.assertNotEqual(worker.tid, current_thread().ident)
        self.assertEqual(worker.pid, os.getpid())
        self.assertFalse(worker.impl.daemon)
        self.assertFalse(worker.is_monitor())
        self.assertEqual(str(worker.impl), worker.impl.unique_name)

    def testWorkerMonitor(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        monitor = worker.monitor
        self.assertEqual(mailbox.address, monitor.address)

    def test_TestSuiteMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(len(arbiter.monitors) >= 1)
        monitor = arbiter.registered['test']
        app = monitor.app
        self.assertTrue(isinstance(app, TestSuite))

    def test_mailbox(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        self.assertTrue(mailbox)
        self.assertTrue(hasattr(mailbox, 'request'))
        self.assertTrue(mailbox._loop)
        self.assertTrue(mailbox._loop.is_running())
        self.assertEqual(worker._loop, mailbox._loop)
        self.assertTrue(mailbox.address)
        self.assertTrue(mailbox.name)

    def test_suite_event_loop(self):
        '''Test event loop in test worker'''
        worker = pulsar.get_actor()
        loop = get_event_loop()
        self.assertTrue(loop.is_running())
        self.assertTrue(worker._loop.is_running())
        self.assertNotEqual(worker._loop, loop)

    def test_yield(self):
        '''Yielding a future calling back on separate thread'''
        worker = pulsar.get_actor()
        loop = get_event_loop()
        loop_tid = yield from pulsar.loop_thread_id(loop)
        self.assertNotEqual(worker.tid, current_thread().ident)
        self.assertEqual(loop_tid, current_thread().ident)
        yield None
        self.assertEqual(loop_tid, current_thread().ident)
        d = Future(loop=worker._loop)

        # We are calling back the future in the event_loop which is on
        # a separate thread
        def _callback():
            d.set_result(current_thread().ident)
        worker._loop.call_soon_threadsafe(
            worker._loop.call_later, 0.2, _callback)
        result = yield from d
        self.assertEqual(worker.tid, result)
        self.assertNotEqual(worker.tid, current_thread().ident)
        self.assertEqual(loop_tid, current_thread().ident)

    def test_unknown_send_target(self):
        # The target does not exists
        result = yield from pulsar.send('vcghdvchdgcvshcd', 'ping')
        self.assertEqual(result, None)

    def test_multiple_execute(self):
        m = yield from multi_async((send('arbiter', 'run', wait, 1.2),
                                    send('arbiter', 'ping'),
                                    send('arbiter', 'echo', 'ciao!'),
                                    send('arbiter', 'run', wait, 2.1),
                                    send('arbiter', 'echo', 'ciao again!')))
        self.assertTrue(m[0] >= 1.1)
        self.assertEqual(m[1], 'pong')
        self.assertEqual(m[2], 'ciao!')
        self.assertTrue(m[3] >= 2.0)
        self.assertEqual(m[4], 'ciao again!')

    def test_tasks(self):
        worker = pulsar.get_actor()
        backend = worker.app.backend
        self.assertTrue(worker.app.backend)
        self.assertEqual(backend.name, worker.app.name)
        self.assertEqual(len(backend.registry), 1)
        self.assertTrue('test' in backend.registry)

    def test_no_plugins(self):
        suite = TestSuite()
        self.assertFalse(suite.cfg.plugins)
        self.assertFalse('profile' in suite.cfg.settings)

    def test_profile_plugins(self):
        suite = TestSuite(plugins=[profile.Profile()])
        self.assertTrue(suite.cfg.plugins)
        self.assertTrue('profile' in suite.cfg.settings)
        self.assertTrue('profile_stats_path' in suite.cfg.settings)

    def test_version(self):
        self.assertTrue(pulsar.VERSION)
        self.assertTrue(pulsar.__version__)
        self.assertEqual(pulsar.__version__, get_version(pulsar.VERSION))
        self.assertTrue(len(pulsar.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))
