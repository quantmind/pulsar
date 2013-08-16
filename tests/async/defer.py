'''Deferred and asynchronous tools.'''
import sys
import time
from functools import reduce

from pulsar import InvalidStateError, Deferred, is_async, NOT_DONE,\
                     is_failure, MultiDeferred, maybe_async, CancelledError,\
                        async_sleep, Failure, safe_async
from pulsar.async.defer import is_exc_info_error
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest


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
    
    def set_error(self):
        try:
            raise ValueError('Bad callback')
        except Exception as e:
            self.callback(e)

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
        except Exception as e:
            trace = sys.exc_info()
            d.callback(e)
        self.assertTrue(d.done())
        self.assertTrue(cbk.done())
        self.assertEqual(cbk.result.exc_info, trace)
        
    def testDeferredCallback(self):
        d = Deferred()
        d.add_callback(lambda r : Cbk(r))
        self.assertFalse(d.done())
        result = d.callback('ciao')
        self.assertTrue(d.done())
        self.assertEqual(d.paused,1)
        self.assertTrue(is_async(result))
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
        result = d.callback('ciao')
        self.assertTrue(d.done())
        self.assertEqual(d.paused, 1)
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
        self.assertEqual(d.result,('ciao','luca','second'))
        
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
        rd.set_error()
        self.assertFalse(d.paused)
        self.assertTrue(is_failure(d.result))
        yield a
        self.assertTrue(a.done())
        self.assertTrue(is_failure(a.result[0]))
        
    def testCancel(self):
        d = Deferred()
        d.cancel('timeout')
        self.assertTrue(d.done())
        self.assertTrue(d.cancelled())
        self.assertTrue(is_failure(d.result))
        
    def testCancelTask(self):
        d = Deferred()
        def gen():
            yield d
        task = maybe_async(gen())
        self.assertNotEqual(task, d)
        task.cancel('timeout')
        self.assertTrue(task.done())
        self.assertTrue(task.cancelled())
        self.assertTrue(is_failure(task.result))
        self.assertTrue(d.cancelled())
        self.assertEqual(str(d), 'Deferred (cancelled)')
        
    def testTimeout(self):
        try:
            yield Deferred(timeout=1)
        except Exception as e:
            self.assertIsInstance(e, CancelledError)

    def test_last_yeild(self):
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
        self.assertTrue(is_async(res))
        self.assertEqual(d.result, res)
        try:
            yield res
        except Exception as e:
            self.assertIsInstance(e, CancelledError)
        
    def test_chain(self):
        d1 = Deferred()
        d2 = d1.then()
        self.assertNotEqual(d1, d2)
        d2.add_callback(lambda res: res+1)
        d1.callback(1)
        self.assertEqual(d1.result, 1)
        self.assertEqual(d2.result, 2)
        

class TestFailure(unittest.TestCase):
    
    def testRepr(self):
        failure = Failure(Exception('test'))
        val = str(failure)
        self.assertEqual(repr(failure), val)
        self.assertTrue('Exception: test' in val)
        
    def testRemote(self):
        failure = Failure(Exception('test'))
        s1 = str(failure)
        self.assertFalse(failure.is_remote)
        failure.logged = True
        remote = pickle.loads(pickle.dumps(failure))
        self.assertTrue(remote.is_remote)
        s2 = str(remote)
        self.assertEqual(s1, s2)
        self.assertTrue(remote.logged)
        
        
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
        self.assertFalse(is_async(r))
        self.assertEqual(r, [[1,2,3,4,5,6,7,8,9,10]])
        
    def testNestedhandle(self):
        handle = lambda value : reduce(lambda x,y: x+y, value)\
                     if isinstance(value, list) else value 
        d = MultiDeferred(handle_value=handle)
        d.append([a for a in range(1,11)])
        r = maybe_async(d.lock())
        self.assertFalse(is_async(r))
        self.assertEqual(r, [55])
        handle = lambda value: 'c'*value
        d = MultiDeferred(handle_value=handle, raise_on_error=False)
        d.append([a for a in range(1,11)])
        r = maybe_async(d.lock())
        self.assertFalse(is_async(r))
        self.assertTrue(is_failure(r[0]))
    

class TestFunctions(unittest.TestCase):
    
    def test_is_exc_info_error(self):
        self.assertFalse(is_exc_info_error(None))
        self.assertFalse(is_exc_info_error((1,2)))
        self.assertFalse(is_exc_info_error((1,2,3)))
        self.assertFalse(is_exc_info_error((None, None, None)))
        try:
            raise ValueError
        except:
            self.assertTrue(is_exc_info_error(sys.exc_info()))
            
    def test_async_sleep(self):
        start = time.time()
        result = yield async_sleep(1)
        self.assertEqual(result, 1)
        self.assertTrue(time.time() - start > 1)
        
    def test_safe_async(self):
        def f():
            raise ValueError
        result = safe_async(f)
        self.assertIsInstance(result, Deferred)
        self.assertTrue(result.done())
        result = result.result
        self.assertIsInstance(result, Failure)
        self.assertIsInstance(result.error, ValueError)