import unittest

from pulsar.api import get_proxy, spawn, send, CommandNotFound


class TestApi(unittest.TestCase):

    def test_get_proxy(self):
        self.assertRaises(ValueError, get_proxy, 'shcbjsbcjcdcd')
        self.assertEqual(get_proxy('shcbjsbcjcdcd', safe=True), None)

    async def test_bad_concurrency(self):
        # bla concurrency does not exists
        with self.assertRaises(ValueError):
            await spawn(kind='bla')

    async def test_actor_coverage(self):
        with self.assertRaises(CommandNotFound):
            await send('arbiter', 'sjdcbhjscbhjdbjsj', 'bla')
