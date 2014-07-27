import unittest

from pulsar import (Future, maybe_async, coroutine_return, chain_future,
                    get_event_loop, yield_from, From)


def c_summation(value):
    result = yield From(value)
    coroutine_return(result + 2)


def coro1():
    done = yield 3
    fut = Future()
    fut._loop.call_soon(fut.set_exception, ValueError('test'))
    try:
        yield fut
    except ValueError:
        done += 1
    coroutine_return(done)


class TestCoroFuture(unittest.TestCase):

    def test_yield_from(self):
        result = yield yield_from(coro1())
        self.assertEqual(result, 4)

    def test_coroutine1(self):
        loop = get_event_loop()
        d1 = Future()
        loop.call_later(0.2, d1.set_result, 1)
        a = yield c_summation(d1)
        self.assertEqual(a, 3)
        self.assertEqual(d1.result(), 1)

    def test_chain(self):
        loop = get_event_loop()
        future = Future()
        next = chain_future(future, callback=lambda r: r+2)
        loop.call_later(0.2, future.set_result, 1)
        result = yield next
        self.assertEqual(result, 3)
