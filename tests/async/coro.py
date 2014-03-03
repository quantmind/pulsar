import unittest

from pulsar import (Future, maybe_async, coroutine_return, chain_future,
                    get_event_loop)


def c_summation(value):
    result = yield value
    coroutine_return(result + 2)


class TestCoroFuture(unittest.TestCase):

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

    def __test_fail_coroutine1(self):
        d1 = Future()
        a = c_summation(d1)
        d1.set_result('bla')
        self.assertEqual(d1.result(), 'bla')
        self.async.assertRaises(TypeError, c_summation, d1)
