'''Test a single test function on a module with only __init__.py'''
import unittest
import asyncio

from pulsar import send


class TestOneAsync(unittest.TestCase):

    @asyncio.coroutine
    def test_ping_monitor(self):
        future = yield from send('test', 'ping')
        self.assertEqual(future, 'pong')
