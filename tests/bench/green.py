from random import randint
import unittest

from pulsar import send
from pulsar.apps import greenio

from examples.echo.manage import server, Echo
from tests.apps.greenio import EchoGreen


class TestGreenIo(unittest.TestCase):
    __benchmark__ = True
    __number__ = 1000

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0')
        cls.server_cfg = yield from send('arbiter', 'run', s)
        cls.client = Echo(cls.server_cfg.addresses[0])
        cls.green = EchoGreen(cls.server_cfg.addresses[0])
        cls.msg = b''.join((b'a' for x in range(2**13)))
        cls.pool = greenio.GreenPool()

    def test_yield_io(self):
        msg = yield from self.client(self.msg)
        self.assertEqual(result, msg)

    @greenio.run_in_greenlet
    def test_green_io(self):
        msg = self.green(self.msg)
        self.assertEqual(result, msg)

    def test_green_pool(self):
        msg = yield from self.pool.submit(self.green, self.msg)
        self.assertEqual(result, msg)
