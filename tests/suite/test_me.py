'''Tests the test suite and pulsar distribution.'''
import unittest
import asyncio

import pulsar
from pulsar import send, multi_async
from pulsar.apps.test import TestSuite
from pulsar.apps.test.plugins import profile
from pulsar.utils.version import get_version


def simple_function(actor):
    return actor.name


@asyncio.coroutine
def wait(actor, period=0.5):
    start = actor._loop.time()
    yield from asyncio.sleep(period)
    return actor._loop.time() - start


class TestTestWorker(unittest.TestCase):

    def test_TestSuiteMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(len(arbiter.monitors) >= 1)
        monitor = arbiter.registered['test']
        app = monitor.app
        self.assertTrue(isinstance(app, TestSuite))

    @asyncio.coroutine
    def test_unknown_send_target(self):
        # The target does not exists
        yield from self.wait.assertRaises(pulsar.CommandError, send,
                                          'vcghdvchdgcvshcd', 'ping')

    @asyncio.coroutine
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

    def test_no_plugins(self):
        suite = TestSuite(test_plugins=[])
        self.assertFalse(suite.cfg.test_plugins)
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
        for m in ("__version__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))
