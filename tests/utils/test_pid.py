import os
import unittest
import asyncio

from pulsar import send
from pulsar.apps.test import ActorTestMixin
from pulsar.utils.tools import Pidfile


class TestPidfile(ActorTestMixin, unittest.TestCase):
    concurrency = 'process'

    @asyncio.coroutine
    def test_create_pid(self):
        proxy = yield from self.spawn_actor(name='pippo')
        info = yield from send(proxy, 'info')
        result = info['actor']
        self.assertTrue(result['is_process'])
        pid = result['process_id']
        #
        p = Pidfile()
        self.assertEqual(p.fname, None)
        self.assertEqual(p.pid, None)
        p.create(pid)
        self.assertTrue(p.fname)
        self.assertEqual(p.pid, pid)
        p1 = Pidfile(p.fname)
        self.assertRaises(RuntimeError, p1.create, p.pid+1)
        #
        p1 = Pidfile('bla/ksdcskcbnskcdbskcbksdjcb')
        self.assertRaises(RuntimeError, p1.create, p.pid+1)
        p1.unlink()
        p.unlink()
        self.assertFalse(os.path.exists(p.fname))
