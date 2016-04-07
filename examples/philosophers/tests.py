import unittest
import asyncio

from pulsar import send
from pulsar.apps.test import test_timeout

from examples.philosophers.manage import DiningPhilosophers


class TestPhylosophers(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    @asyncio.coroutine
    def setUpClass(cls):
        app = DiningPhilosophers(name='plato',
                                 concurrency=cls.concurrency)
        cls.app_cfg = yield from send('arbiter', 'run', app)

    @test_timeout(30)
    @asyncio.coroutine
    def test_info(self):
        while True:
            yield from asyncio.sleep(0.5)
            info = yield from send('plato', 'info')
            all = []
            for data in info.get('workers', []):
                p = data.get('philosopher')
                if p:
                    all.append(p)
            if len(all) == 5:
                break

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)
