import unittest

from pulsar import new_event_loop, coroutine_return, Future


DELAY = 0


def async_func(loop, value):
    p = Future(loop)
    loop.call_later(DELAY, p.set_result, value)
    return p


def sub_sub(loop, num):
    a = yield async_func(loop, num)
    b = yield async_func(loop, num)
    yield 0
    coroutine_return(a+b)


def sub(loop, num):
    a = yield async_func(loop, num)
    b = yield async_func(loop, num)
    c = yield sub_sub(loop, num)
    coroutine_return(a+b+c)


def main(d, loop, num):
    a = yield async_func(loop, num)
    b = yield sub(loop, num)
    c = yield sub(loop, num)
    d.set_result(a+b+c)


class TestCoroutine(unittest.TestCase):
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
