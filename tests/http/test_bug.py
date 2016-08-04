import unittest

from pulsar.apps import http


class TestBugs(unittest.TestCase):

    async def test_json(self):
        c = http.HttpClient()
        data = await c.get('https://api.bitfinex.com/v1/pubticker/BTCUSD')
        self.assertTrue(data)
