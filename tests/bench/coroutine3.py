import sys
import unittest

from pulsar import Future, new_event_loop, coroutine_return

DELAY = 0

def async_func(loop, value):
    p = Future(loop)
    loop.call_later(DELAY, p.callback, value)
    return p


def sub_sub(loop, num):
    a = yield from async_func(loop, num)
    b = yield from async_func(loop, num)
    yield 0
    return a + b


def sub(loop, num):
    a = yield from async_func(loop, num)
    b = yield from async_func(loop, num)
    c = yield from sub_sub(loop, num)
    coroutine_return(a+b+c)


def main(d, loop, num):
    try:
        a = yield from async_func(loop, num)
        b = yield from sub(loop, num)
        c = yield from sub(loop, num)
    except Exception:
        d.callback(sys.exc_info())
    else:
        d.callback(a+b+c)


class TestCoroutine33(unittest.TestCase):
    __benchmark__ = True
    __number__ = 100

    def setUp(self):
        self.loop = new_event_loop()

    def test_coroutine(self):
        future = Future(self.loop)
        self.loop.call_soon(main, future, self.loop, 1)
        self.loop.run_until_complete(future)
        self.assertEqual(future.result(), 9)

    def getTime(self, dt):
        return dt - 9*DELAY
