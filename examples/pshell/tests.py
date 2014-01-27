import unittest
import time

from pulsar import async_while, send, get_actor
from pulsar.apps.test import run_on_arbiter
from pulsar.apps.shell import InteractiveConsole, decode_line, PulsarShell


class DummyConsole(InteractiveConsole):

    def setup(self):
        pass

    def interact(self, timeout):
        time.sleep(timeout)


def start(actor):
    shell = PulsarShell(console_class=DummyConsole, workers=2)
    return shell(actor)


class TestShell(unittest.TestCase):
    app_cfg = None

    @classmethod
    def setUpClass(cls):
        cls.app_cfg = yield send('arbiter', 'run', start)

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    def test_app(self):
        cfg = self.app_cfg
        self.assertEqual(cfg.name, 'shell')
        self.assertEqual(cfg.callable, None)
        self.assertEqual(cfg.console_class, DummyConsole)
        self.assertEqual(cfg.workers, 0)
        self.assertEqual(cfg.thread_workers, 1)
        self.assertEqual(cfg.concurrency, 'thread')
        self.assertEqual(decode_line('bla'), 'bla')

    @run_on_arbiter
    def test_test_worker(self):
        arbiter = get_actor()
        monitor = arbiter.get_actor('shell')
        yield async_while(2, lambda: not monitor.managed_actors)
        self.assertEqual(len(monitor.managed_actors), 0)
