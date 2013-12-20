'''Deferred and asynchronous tools.'''
import sys
from functools import reduce

from pulsar import (InvalidStateError, Deferred, NOT_DONE,
                    is_failure, maybe_async, CancelledError,
                    async_sleep, Failure, safe_async, InvalidStateError,
                    coroutine_return, async, TimeoutError)
from pulsar.utils.pep import pickle, default_timer
from pulsar.apps.test import unittest, mute_failure


class Cbk(Deferred):
    '''A deferred object'''
    def __init__(self, r=None):
        super(Cbk, self).__init__()
        if r is not None:
            self.r = (r,)
        else:
            self.r = ()

    def add(self, result):
        self.r += (result,)
        return self

    def set_result(self, result):
        self.add(result)
        self.callback(self.r)


def simple_error():
    yield ValueError('Kaput!')


def async_pair():
    c = Deferred()
    d = Deferred().add_both(c.callback)
    return d, c


class TestDeferred(unittest.TestCase):

    def testSimple(self):
        d = Deferred()
        self.assertFalse(d.done())
        self.assertEqual(str(d), 'Deferred (pending)')
        d.callback('ciao')
        self.assertTrue(d.done())
        self.assertTrue(' (done)' in str(d))
        self.assertEqual(d.result(), 'ciao')
        self.assertRaises(InvalidStateError, d.callback, 'bla')

    def testBadCallback(self):
        d = Deferred()
        self.assertRaises(TypeError, d.add_callback, 3)
        self.assertRaises(TypeError, d.add_callback, lambda r: r, 4)

    def test_None_callback(self):
        d = Deferred()
        d.add_callback(None)
        self.assertEqual(d.has_callbacks(), 0)
        d.add_callback(None, None)
        self.assertEqual(d.has_callbacks(), 0)
        errback = lambda r: r
        d.add_callback(None, errback)
        self.assertTrue(d.has_callbacks())

    def testWrongOperations(self):
        d = Deferred()
        self.assertRaises(RuntimeError, d.callback, Deferred())

    def testCallbacks(self):
        d, cbk = async_pair()
        self.assertFalse(d.done())
        d.callback('ciao')
        self.assertTrue(d.done())
        self.assertEqual(cbk.result(), 'ciao')

    def testError(self):
        d, cbk = async_pair()
        self.assertFalse(d.done())
        try:
            raise Exception('blabla exception')
        except Exception:
            d.set_exception(sys.exc_info())
        self.assertTrue(d.done())
        self.assertTrue(cbk.done())
        self.assertIsInstance(cbk.exception(), Failure)

    def test_deferred_callback(self):
        d = Deferred()
        d.add_callback(lambda r: Cbk(r))
        self.assertFalse(d.done())
        result = d.callback('ciao')
        self.assertTrue(d.done())
        self.assertEqual(d._paused, 1)
        self.assertIsInstance(result, Deferred)
        self.assertEqual(result.has_callbacks(), 1)
        self.assertFalse(result.done())
        result.set_result('luca')
        self.assertTrue(result.done())
        self.assertEqual(result.result(), ('ciao', 'luca'))
        self.assertEqual(d._paused, 0)

    def testDeferredCallbackInGenerator(self):
        d = Deferred()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r: rd.add(r))
        d.add_callback(lambda r: r + ('second',))

        def _gen():
            yield d

        a = maybe_async(_gen())
        # The waiting deferred is the original deferred
        self.assertEqual(a._waiting, d)
        # Callback d
        result = d.callback('ciao')
        self.assertTrue(d.done())
        self.assertFalse(a.done())
        self.assertEqual(d._paused, 1)
        # still the same deferred
        self.assertEqual(a._waiting, d)
        #
        self.assertEqual(d.has_callbacks(), 2)
        self.assertEqual(rd.has_callbacks(), 1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.done())
        #
        # set callback
        rd.set_result('luca')
        # release the loop
        yield a
        self.assertTrue(a.done())
        self.assertFalse(d._paused)
        self.assertEqual(d.result(), ('ciao', 'luca', 'second'))

    def testDeferredErrorbackInGenerator(self):
        d = Deferred()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r: rd.add(r))
        d.add_callback(lambda r: r + ('second',))

        def _gen():
            yield d

        a = maybe_async(_gen()).add_errback(lambda failure: [failure])
        result = d.callback('ciao')  # first callback
        self.assertTrue(d.done())
        self.assertEqual(d._paused, 1)
        # The generator has added its consume callback
        self.assertEqual(d.has_callbacks(), 2)
        self.assertEqual(rd.has_callbacks(), 1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.done())
        #
        # set Error back
        rd.set_exception(ValueError('Bad callback'))
        self.assertFalse(d._paused)
        self.assertTrue(is_failure(d.exception()))
        yield a
        self.assertTrue(a.done())
        self.assertTrue(is_failure(a.result()[0]))
        mute_failure(self, a.result()[0])

    def test_cancel(self):
        d = Deferred()
        self.assertFalse(d._timeout)
        d.cancel('timeout')
        self.assertTrue(d.done())
        self.assertTrue(d.cancelled())
        self.assertTrue(is_failure(d.exception()))

    def testCancelTask(self):
        d = Deferred()

        def gen():
            yield d

        task = maybe_async(gen())
        self.assertNotEqual(task, d)
        task.cancel('timeout')
        try:
            yield task
        except CancelledError:
            pass
        self.assertTrue(task.done())
        self.assertTrue(task.cancelled())
        self.assertTrue(d.cancelled())
        self.assertTrue(is_failure(task.exception()))
        self.assertEqual(str(d), 'Deferred (cancelled)')

    def testTimeout(self):
        d = Deferred().set_timeout(1)
        self.assertTrue(d._timeout)
        try:
            yield d
        except TimeoutError:
            pass

    def test_last_yield(self):
        def gen():
            try:
                yield Deferred().set_timeout(1)
            except TimeoutError:
                coroutine_return('OK')
        result = yield gen()
        self.assertEqual(result, 'OK')

    def test_embedded_timeout(self):
        '''Embed an never ending coroutine into a deferred with timeout'''
        def gen(r):
            # A never ending coroutine.
            while True:
                yield NOT_DONE
        d = Deferred().set_timeout(1).add_callback(gen)
        res = d.callback(True)
        self.assertIsInstance(res, Deferred)
        self.assertEqual(d.result(), res)
        try:
            yield res
        except TimeoutError:
            pass

    def test_then(self):
        d1 = Deferred()
        d2 = d1.then()
        self.assertNotEqual(d1, d2)
        d2.add_callback(lambda res: res+1)
        d1.callback(1)
        self.assertEqual(d1.result(), 1)
        self.assertEqual(d2.result(), 2)

    def test_throw(self):
        d = Deferred()
        d.callback(1)
        self.assertEqual(d.result(), 1)
        d = Deferred()
        d.callback(ValueError())
        exc = d.exception()
        self.assertTrue(exc.logged)
        self.assertRaises(ValueError, d.result)

    def test_cancellation(self):
        d = Deferred()
        d.cancel()
        self.assertTrue(d.cancelled())
        failure = d.exception()
        self.assertTrue(failure.isinstance(CancelledError))
        self.assertEqual(d.callback(3), failure)
        self.assertRaises(InvalidStateError, d.callback, 3)

    def test_coroutine_return(self):
        def f(count=0):
            yield -1
            coroutine_return('a')
            for i in range(count):
                yield i
        result = yield f()
        self.assertEqual(result, 'a')
        result = yield f(10)
        self.assertEqual(result, 'a')

    def test_async_error(self):
        d = async(simple_error())
        self.assertIsInstance(d, Deferred)
        self.assertTrue(d.done())
        self.assertIsInstance(d.exception(), Failure)

    def test_async_sleep(self):
        start = default_timer()
        result = yield async_sleep(2.1)
        self.assertEqual(result, 2.1)
        self.assertTrue(default_timer() - start > 2.1)

    def test_async_sleep_callback(self):
        start = default_timer()
        result = yield async_sleep(1.1).add_callback(async_sleep)
        self.assertTrue(default_timer() - start >= 2.2)
        self.assertEqual(result, 1.1)

    def test_safe_async(self):
        def f():
            raise ValueError('test safe async')
        result = safe_async(f)
        self.assertIsInstance(result, Deferred)
        self.assertTrue(result.done())
        result = result.exception()
        self.assertIsInstance(result, Failure)
        self.assertIsInstance(result.error, ValueError)
