import unittest
import asyncio


def coro(msg):
    future = asyncio.Future()
    future._loop.call_later(1, future.set_result, msg)
    result = yield from future
    return result


class TestCoroutines(unittest.TestCase):

    def test_yield_from(self):
        result = yield from coro('Hi')
        self.assertEqual(result, 'Hi')
