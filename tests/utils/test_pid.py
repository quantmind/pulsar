import os
import unittest

from pulsar import send
from pulsar.apps.test import ActorTestMixin
from pulsar.utils.tools import Pidfile


class TestPidfile(ActorTestMixin, unittest.TestCase):
    concurrency = 'process'

    async def test_create_pid(self):
        proxy = await self.spawn_actor(name='pippo')
        info = await send(proxy, 'info')
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
        self.assertTrue(p.exists)
        #
        p1 = Pidfile(p.fname)
        self.assertRaises(RuntimeError, p1.create, p.pid+1)
        #
        p1 = Pidfile('bla/ksdcskcbnskcdbskcbksdjcb')
        self.assertRaises(RuntimeError, p1.create, p.pid+1)
        p1.unlink()
        p.unlink()
        self.assertFalse(os.path.exists(p.fname))

    def test_stale_pid(self):
        p = Pidfile()
        p.create(798797)
        self.assertTrue(p.fname)
        self.assertEqual(p.pid, 798797)
        self.assertFalse(p.exists)
        #
        # Now create again with different pid
        p.create(798798)
        self.assertEqual(p.pid, 798798)
        self.assertFalse(p.exists)
        p.unlink()
