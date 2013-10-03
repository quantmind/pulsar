'''Deferred and asynchronous tools.'''
import sys
from functools import reduce

from pulsar import (InvalidStateError, Deferred, NOT_DONE,
                    is_failure, MultiDeferred, maybe_async, CancelledError,
                    async_sleep, Failure, safe_async, async,
                    InvalidStateError, coroutine_return)
from pulsar.async.defer import is_exc_info
from pulsar.utils.pep import pickle, default_timer
from pulsar.apps.test import unittest, mute_failure


class Cbk(Deferred):
    '''A deferred object'''
    def __init__(self, r = None):
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


@async()
def simple_error():
    raise ValueError('Kaput!')

def async_pair():
    c = Deferred()
    d = Deferred().add_both(c.callback)
    return d, c


class TestDeferred(unittest.TestCase):

    def testSimple(self):
        d = Deferred()
        self.assertFalse(d.done())
        self.assertFalse(d.running())
        self.assertEqual(str(d), 'Deferred (pending)')
        d.callback('ciao')
        self.assertTrue(d.done())
        self.assertTrue(' (done)' in str(d))
        self.assertEqual(d.result, 'ciao')
        self.assertRaises(InvalidStateError, d.callback, 'bla')

    def testBadCallback(self):
        d = Deferred()
        self.assertRaises(TypeError, d.add_callback, 3)
        self.assertRaises(TypeError, d.add_callback, lambda r: r, 4)

    def test_None_callback(self):
        d = Deferred()
        d.add_callback(None)
        self.assertEqual(d._callbacks, None)
        d.add_callback(None, None)
        self.assertEqual(d._callbacks, None)
        errback = lambda r: r
        d.add_callback(None, errback)
        self.assertTrue(d._callbacks)
        self.assertEqual(d._callbacks[0][:2], (None, errback))

    def testWrongOperations(self):
        d = Deferred()
        self.assertRaises(RuntimeError, d.callback, Deferred())

    def testCallbacks(self):
        d, cbk = async_pair()
        self.assertFalse(d.done())
        d.callback('ciao')
        self.assertTrue(d.done())
        self.assertEqual(cbk.result, 'ciao')

    def testError(self):
        d, cbk = async_pair()
        self.assertFalse(d.done())
        try:
            raise Exception('blabla exception')
        except Exception:
            d.callback(sys.exc_info())
        self.assertTrue(d.done())
        self.assertTrue(cbk.done())
        self.assertIsInstance(cbk.result, Failure)
        mute_failure(self, cbk.result)

    def testDeferredCallback(self):
        d = Deferred()
        d.add_callback(lambda r : Cbk(r))
        self.assertFalse(d.done())
        result = d.callback('ciao')
        self.assertTrue(d.done())
        self.assertEqual(d.paused,1)
        self.assertIsInstance(result, Deferred)
        self.assertEqual(len(result._callbacks),1)
        self.assertFalse(result.done())
        result.set_result('luca')
        self.assertTrue(result.done())
        self.assertEqual(result.result,('ciao','luca'))
        self.assertEqual(d.paused,0)

    def testDeferredCallbackInGenerator(self):
        d = Deferred()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r : rd.add(r))
        d.add_callback(lambda r : r + ('second',))
        def _gen():
            yield d
        a = maybe_async(_gen())
        # The waiting deferred is the original deferred
        self.assertEqual(a._waiting, d)
        # Callback d
        result = d.callback('ciao')
        self.assertTrue(d.done())
        self.assertFalse(a.done())
        self.assertEqual(d.paused, 1)
        # still the same deferred
        self.assertEqual(a._waiting, d)
        #
        self.assertEqual(len(d._callbacks), 2)
        self.assertEqual(len(rd._callbacks), 1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.done())
        #
        # set callback
        rd.set_result('luca')
        # release the loop
        yield a
        self.assertTrue(a.done())
        self.assertFalse(d.paused)
        self.assertEqual(d.result,('ciao', 'luca', 'second'))

    def testDeferredErrorbackInGenerator(self):
        d = Deferred()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r : rd.add(r))
        d.add_callback(lambda r : r + ('second',))
        def _gen():
            yield d
        a = maybe_async(_gen()).add_errback(lambda failure: [failure])
        result = d.callback('ciao') # first callback
        self.assertTrue(d.done())
        self.assertEqual(d.paused, 1)
        # The generator has added its consume callback
        self.assertEqual(len(d._callbacks), 2)
        self.assertEqual(len(rd._callbacks), 1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.done())
        #
        # set Error back
        rd.set_exception(ValueError('Bad callback'))
        self.assertFalse(d.paused)
        self.assertTrue(is_failure(d.result))
        yield a
        self.assertTrue(a.done())
        self.assertTrue(is_failure(a.result[0]))
        mute_failure(self, a.result[0])

    def testCancel(self):
        d = Deferred()
        self.assertFalse(d.timeout)
        d.cancel('timeout')
        self.assertTrue(d.done())
        self.assertTrue(d.cancelled())
        self.assertTrue(is_failure(d.result))
        mute_failure(self, d.result)

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
        self.assertTrue(is_failure(task.result))
        self.assertTrue(d.cancelled())
        self.assertEqual(str(d), 'Deferred (cancelled)')
        mute_failure(self, task.result)

    def testTimeout(self):
        d = Deferred(timeout=1)
        self.assertTrue(d.timeout)
        try:
            yield d
        except Exception as e:
            self.assertIsInstance(e, CancelledError)

    def test_last_yield(self):
        def gen():
            try:
                yield Deferred(timeout=1)
            except CancelledError:
                yield 'OK'
        result = yield gen()
        self.assertEqual(result, 'OK')

    def test_embedded_timeout(self):
        '''Embed an never ending coroutine into a deferred with timeout'''
        def gen(r):
            # A never ending coroutine.
            while True:
                yield NOT_DONE
        d = Deferred(timeout=1).add_callback(gen)
        res = d.callback(True)
        self.assertIsInstance(res, Deferred)
        self.assertEqual(d.result, res)
        try:
            yield res
        except Exception as e:
            self.assertIsInstance(e, CancelledError)

    def test_then(self):
        d1 = Deferred()
        d2 = d1.then()
        self.assertNotEqual(d1, d2)
        d2.add_callback(lambda res: res+1)
        d1.callback(1)
        self.assertEqual(d1.result, 1)
        self.assertEqual(d2.result, 2)

    def test_throw(self):
        d = Deferred()
        d.throw()
        d.callback(1)
        d.throw()
        d = Deferred()
        d.callback(ValueError())
        d.result.mute()
        self.assertRaises(ValueError, d.throw)
        self.assertTrue(d.result.logged)

    def test_cancellation(self):
        d = Deferred()
        d.cancel()
        self.assertTrue(d.cancelled())
        failure = d.result
        self.assertTrue(failure.isinstance(CancelledError))
        self.assertEqual(d.callback(3), failure)
        self.assertRaises(InvalidStateError, d.callback, 3)
        mute_failure(self, d.result)


class TestMultiDeferred(unittest.TestCase):

    def testSimple(self):
        d = MultiDeferred()
        self.assertFalse(d.done())
        self.assertFalse(d._locked)
        self.assertFalse(d._deferred)
        self.assertFalse(d._stream)
        d.lock()
        self.assertTrue(d.done())
        self.assertTrue(d._locked)
        self.assertEqual(d.result,[])
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(InvalidStateError, d._finish)

    def testMulti(self):
        d = MultiDeferred()
        d1 = Deferred()
        d2 = Deferred()
        d.append(d1)
        d.append(d2)
        d.append('bla')
        self.assertRaises(RuntimeError, d._finish)
        d.lock()
        self.assertRaises(RuntimeError, d._finish)
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(RuntimeError, d.append, d1)
        self.assertFalse(d.done())
        d2.callback('first')
        self.assertFalse(d.done())
        d1.callback('second')
        self.assertTrue(d.done())
        self.assertEqual(d.result,['second', 'first', 'bla'])

    def testUpdate(self):
        d1 = Deferred()
        d2 = Deferred()
        d = MultiDeferred()
        d.update((d1,d2)).lock()
        d1.callback('first')
        d2.callback('second')
        self.assertTrue(d.done())
        self.assertEqual(d.result,['first','second'])

    def testNested(self):
        d = MultiDeferred()
        # add a generator
        d.append([a for a in range(1,11)])
        r = maybe_async(d.lock())
        self.assertTrue(d.locked)
        self.assertNotIsInstance(r, Deferred)
        self.assertEqual(r, [[1,2,3,4,5,6,7,8,9,10]])


class TestCoroutine(unittest.TestCase):

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


class TestFunctions(unittest.TestCase):

    def test_is_exc_info(self):
        self.assertFalse(is_exc_info(None))
        self.assertFalse(is_exc_info((1,2)))
        self.assertFalse(is_exc_info((1,2,3)))
        self.assertFalse(is_exc_info((None, None, None)))
        try:
            raise ValueError
        except:
            self.assertTrue(is_exc_info(sys.exc_info()))

    def test_async_error(self):
        d = simple_error()
        self.assertIsInstance(d, Deferred)
        self.assertTrue(d.done())
        self.assertIsInstance(d.result, Failure)
        d.result.mute()

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
        result = result.result
        self.assertIsInstance(result, Failure)
        self.assertIsInstance(result.error, ValueError)
        mute_failure(self, result)
