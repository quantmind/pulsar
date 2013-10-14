'''Tests the test suite and pulsar distribution.'''
import os
from threading import current_thread

import pulsar
from pulsar import send, multi_async, is_failure
from pulsar.apps.test import unittest, run_on_arbiter, TestSuite, sequential
from pulsar.apps.test.plugins import bench, profile
from pulsar.utils.pep import get_event_loop, default_timer
from pulsar.utils.version import get_version


def simple_function(actor):
    return actor.name


def wait(actor, period=0.5):
    start = default_timer()
    yield pulsar.async_sleep(period)
    yield default_timer() - start


class TestTestWorker(unittest.TestCase):

    def testWorker(self):
        '''Test the test worker'''
        worker = pulsar.get_actor()
        self.assertTrue(pulsar.is_actor(worker))
        self.assertTrue(worker.is_running())
        self.assertFalse(worker.closed())
        self.assertFalse(worker.stopped())
        self.assertEqual(worker.info_state, 'running')
        self.assertNotEqual(worker.tid, current_thread().ident)
        self.assertEqual(worker.pid, os.getpid())
        self.assertTrue(worker.impl.daemon)
        self.assertFalse(worker.is_monitor())
        self.assertEqual(str(worker.impl), worker.impl.unique_name)

    def testCPUbound(self):
        worker = pulsar.get_actor()
        loop = pulsar.get_request_loop()
        self.assertTrue(loop.cpubound)
        self.assertFalse(worker.event_loop.cpubound)

    def testWorkerMonitor(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        monitor = worker.monitor
        self.assertEqual(mailbox.address, monitor.address)
        self.assertEqual(mailbox.timeout, 0)

    @run_on_arbiter
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
        self.assertTrue(mailbox.event_loop)
        self.assertTrue(mailbox.event_loop.running)
        self.assertEqual(worker.event_loop, mailbox.event_loop)
        self.assertEqual(worker.tid, mailbox.event_loop.tid)
        self.assertTrue(mailbox.address)
        self.assertTrue(mailbox.name)
        self.assertEqual(mailbox.concurrent_connections, 1)

    def test_event_loop(self):
        '''Test event loop in test worker'''
        worker = pulsar.get_actor()
        loop = pulsar.get_request_loop()
        event_loop = get_event_loop()
        self.assertTrue(loop.running)
        self.assertTrue(event_loop.running)
        self.assertNotEqual(loop, event_loop)
        self.assertEqual(worker.event_loop, event_loop)
        self.assertEqual(worker.tid, worker.event_loop.tid)
        self.assertNotEqual(worker.tid, loop.tid)
        self.assertTrue(str(event_loop))

    def test_NOT_DONE(self):
        worker = pulsar.get_actor()
        loop = pulsar.get_request_loop()
        count = loop.num_loops
        yield pulsar.NOT_DONE
        self.assertEqual(loop.num_loops, count+1)
        yield pulsar.NOT_DONE
        self.assertEqual(loop.num_loops, count+2)

    def test_yield(self):
        '''Yielding a deferred calling back on separate thread'''
        worker = pulsar.get_actor()
        loop = pulsar.get_request_loop()
        self.assertNotEqual(worker.tid, current_thread().ident)
        self.assertEqual(loop.tid, current_thread().ident)
        yield pulsar.NOT_DONE
        self.assertEqual(loop.tid, current_thread().ident)
        d = pulsar.Deferred()
        # We are calling back the deferred in the event_loop which is on
        # a separate thread
        def _callback():
            d.callback(current_thread().ident)
        worker.event_loop.call_later(0.2, _callback)
        yield d
        self.assertEqual(worker.tid, d.result)
        self.assertEqual(worker.event_loop.tid, d.result)
        self.assertNotEqual(worker.tid, current_thread().ident)
        self.assertEqual(loop.tid, current_thread().ident)

    def testInline(self):
        val = yield 3
        self.assertEqual(val, 3)
        future = yield send('monitor', 'ping')
        self.assertEqual(future, 'pong')

    def test_run_on_arbiter(self):
        actor = pulsar.get_actor()
        response = yield actor.send('arbiter', 'run', simple_function)
        self.assertEqual(response, 'arbiter')

    def test_unknown_send_target(self):
        # The target does not exists
        future = pulsar.send('vcghdvchdgcvshcd',
                             'ping').add_both(lambda r: [r])
        yield future
        self.assertTrue(is_failure(future.result[0]))

    def test_multiple_execute(self):
        m = yield multi_async((send('arbiter', 'run', wait, 1.2),
                               send('arbiter', 'ping'),
                               send('arbiter', 'echo', 'ciao!'),
                               send('arbiter', 'run', wait, 2.1),
                               send('arbiter', 'echo', 'ciao again!')))
        self.assertTrue(m[0] >= 1.2)
        self.assertEqual(m[1], 'pong')
        self.assertEqual(m[2], 'ciao!')
        self.assertTrue(m[3] >= 2.1)
        self.assertEqual(m[4], 'ciao again!')

    def test_tasks(self):
        worker = pulsar.get_actor()
        backend = worker.app.backend
        self.assertTrue(worker.app.backend)
        self.assertEqual(backend.name, worker.app.name)
        self.assertEqual(len(backend.registry), 1)
        self.assertTrue('test' in backend.registry)


class TestTestSuite(unittest.TestCase):

    def test_no_plugins(self):
        suite = TestSuite()
        self.assertFalse(suite.cfg.plugins)
        self.assertFalse('profile' in suite.cfg.settings)

    def test_profile_plugins(self):
        suite = TestSuite(plugins=[profile.Profile()])
        self.assertTrue(suite.cfg.plugins)
        self.assertTrue('profile' in suite.cfg.settings)
        self.assertTrue('profile_stats_path' in suite.cfg.settings)


class TestPulsar(unittest.TestCase):

    def test_version(self):
        self.assertTrue(pulsar.VERSION)
        self.assertTrue(pulsar.__version__)
        self.assertEqual(pulsar.__version__, get_version(pulsar.VERSION))
        self.assertTrue(len(pulsar.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))
