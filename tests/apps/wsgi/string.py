from pulsar.apps.wsgi import AsyncString
from pulsar.apps.test import unittest


class TestAsyncString(unittest.TestCase):
    
    def test_string(self):
        a = AsyncString ('Hello')
        self.assertEqual(a.render(), 'Hello')
        self.assertRaises(RuntimeError, a.render)
    