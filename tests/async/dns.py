import unittest
import asyncio

from pulsar import get_event_loop


class TestDns(unittest.TestCase):

    @asyncio.coroutine
    def test_getnameinfo(self):
        loop = get_event_loop()
        info = yield from loop.getaddrinfo('www.bbc.co.uk', 'http')
        info = yield from loop.getnameinfo(('212.58.244.66', 80))
        info = yield from loop.getaddrinfo('github.com', 'https')
        self.assertTrue(info)
        addr = info[0][4]
        info = yield from loop.getnameinfo(addr)
        self.assertTrue(info)
