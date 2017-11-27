import unittest

from pulsar.utils.pylib.events import EventHandler as PyEventHandler
from pulsar.api import EventHandler


class TestPythonCode(unittest.TestCase):
    __benchmark__ = True
    __number__ = 10000

    @classmethod
    async def setUpClass(cls):
        cls.hnd = EventHandler()
        cls.py_hnd = PyEventHandler()
        cls.hnd.event('bang').bind(cls.bang)
        cls.py_hnd.event('bang').bind(cls.bang)

    def test_bind(self):
        self.hnd.event('foo').bind(self.foo)

    def test_fire(self):
        self.hnd.fire_event('bang')

    def test_fire_data(self):
        self.hnd.fire_event('bang', data=self)

    def test_bind_py(self):
        self.py_hnd.event('foo').bind(self.foo)

    def test_fire_py(self):
        self.py_hnd.fire_event('bang')

    def test_fire_data_py(self):
        self.py_hnd.fire_event('bang', data=self)

    @classmethod
    def foo(cls, o):
        pass

    @classmethod
    def bang(cls, o, data=None):
        pass
