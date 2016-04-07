import unittest
import asyncio

from pulsar import ensure_future, new_event_loop, Future


DELAY = 0


def async_func(loop, value):
    p = Future(loop=loop)
    loop.call_later(DELAY, p.set_result, value)
    return p


@asyncio.coroutine
def sub_sub(loop, num):
    a = yield from async_func(loop, num)
    b = yield from async_func(loop, num)
    return a+b


@asyncio.coroutine
def sub(loop, num):
    a = yield from async_func(loop, num)
    b = yield from async_func(loop, num)
    c = yield from sub_sub(loop, num)
    return a+b+c


@asyncio.coroutine
def main(loop, num):
    a = yield from async_func(loop, num)
    b = yield from sub(loop, num)
    c = yield from sub(loop, num)
    return a+b+c


class TestCoroutine(unittest.TestCase):
    __benchmark__ = True
    __number__ = 100

    def setUp(self):
        self.loop = new_event_loop()

    def test_coroutine(self):
        future = ensure_future(main(self.loop, 1), loop=self.loop)
        self.loop.run_until_complete(future)
        self.assertEqual(future.result(), 9)

    def getTime(self, dt):
        return dt - 9*DELAY
