'''Deferred and asynchronous tools.'''
import sys

from pulsar import AlreadyCalledError, Deferred, is_async,\
                     make_async, IOLoop, is_failure, MultiDeferred
from pulsar.utils.test import test


class Cbk(Deferred):
    '''A deferred object'''
    def __init__(self, r = None):
        super(Cbk,self).__init__()
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
    c = Cbk()
    d = Deferred().add_callback(c.set_result)
    return d, c
    
class TestDeferred(test.TestCase):
    
    def testSimple(self):
        d = Deferred()
        self.assertEqual(d.rid,None)
        self.assertFalse(d.called)
        self.assertFalse(d.running)
        self.assertEqual(str(d),'Deferred')
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.result,'ciao')
        self.assertRaises(AlreadyCalledError,d.callback,'bla')
        
    def testWrongOperations(self):
        d = Deferred()
        self.assertRaises(RuntimeError, d.callback, Deferred())

    def testCallbacks(self):
        d,cbk = async_pair()
        self.assertFalse(d.called)
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(cbk.result,'ciao')
        
    def testError(self):
        d,cbk = async_pair()
        self.assertFalse(d.called)
        try:
            raise Exception('blabla exception')
        except Exception as e:
            trace = sys.exc_info()
            d.callback(e)
        self.assertTrue(d.called)
        self.assertEqual(cbk.result[-1],trace)
        
    def testDeferredCallback(self):
        d = Deferred()
        d.add_callback(lambda r : Cbk(r))
        self.assertFalse(d.called)
        result = d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.paused,1)
        self.assertTrue(is_async(result))
        self.assertEqual(len(result._callbacks),1)
        self.assertFalse(result.called)
        result.set_result('luca')
        self.assertTrue(result.called)
        self.assertEqual(result.result,('ciao','luca'))
        self.assertEqual(d.paused,0)
        
    def testDeferredCallbackInGenerator(self):
        d = Deferred()
        ioloop = IOLoop()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r : rd.add(r))
        d.add_callback(lambda r : r + ('second',))
        def _gen():
            yield d
        a = make_async(_gen())
        result = d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.paused,1)
        self.assertEqual(len(d._callbacks),1)
        self.assertEqual(len(rd._callbacks),1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.called)
        a.start(ioloop)
        self.assertFalse(a.called)
        # do one loop
        self.assertEqual(len(ioloop._callbacks),1)
        ioloop._callbacks.pop()()
        self.assertFalse(a.called)
        self.assertEqual(d.paused,1)
        self.assertEqual(d.result,rd)
        self.assertEqual(len(d._callbacks),1)
        #
        # set callback
        rd.set_result('luca')
        self.assertTrue(a.called)
        self.assertFalse(d.paused)
        self.assertEqual(d.result,('ciao','luca','second'))
        # do another loop
        self.assertEqual(len(ioloop._callbacks),1)
        ioloop._callbacks.pop()()
        self.assertTrue(a.called)
        self.assertEqual(a.result,d.result)
        
    def testDeferredErrorbackInGenerator(self):
        d = Deferred()
        ioloop = IOLoop()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r : rd.add(r))
        d.add_callback(lambda r : r + ('second',))
        def _gen():
            yield d
        a = make_async(_gen())
        result = d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.paused,1)
        self.assertEqual(len(d._callbacks),1)
        self.assertEqual(len(rd._callbacks),1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.called)
        a.start(ioloop)
        self.assertFalse(a.called)
        # do one loop
        self.assertEqual(len(ioloop._callbacks),1)
        ioloop._callbacks.pop()()
        self.assertFalse(a.called)
        self.assertEqual(d.paused,1)
        self.assertEqual(d.result,rd)
        self.assertEqual(len(d._callbacks),1)
        #
        # set Error back
        rd.set_error()
        self.assertTrue(a.called)
        self.assertFalse(d.paused)
        self.assertTrue(is_failure(d.result))
        # do another loop
        self.assertEqual(len(ioloop._callbacks),1)
        ioloop._callbacks.pop()()
        self.assertTrue(a.called)
        #self.assertEqual(a.result,d.result)
        
        
class TestMultiDeferred(test.TestCase):
    
    def testSimple(self):
        d = MultiDeferred()
        self.assertFalse(d.called)
        self.assertFalse(d._locked)
        self.assertFalse(d._underlyings)
        self.assertFalse(d._results)
        d.lock()
        self.assertTrue(d.called)
        self.assertTrue(d._locked)
        self.assertEqual(d.result,[])
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(RuntimeError, d._finish)
        
    def testMulti(self):
        d = MultiDeferred()
        d1 = Deferred()
        d2 = Deferred()
        d.add(d1)
        d.add(d2)
        self.assertRaises(ValueError, d.add, 'bla')
        self.assertRaises(RuntimeError, d._finish)
        d.lock()
        self.assertRaises(RuntimeError, d._finish)
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(RuntimeError, d.add, d1)
        self.assertFalse(d.called)
        d2.callback('first')
        self.assertFalse(d.called)
        d1.callback('second')
        self.assertTrue(d.called)
        self.assertEqual(d.result,['second','first'])
        
    def testUpdate(self):
        d1 = Deferred()
        d2 = Deferred()
        d = MultiDeferred()
        d.update((d1,d2)).lock()
        d1.callback('first')
        d2.callback('second')
        self.assertTrue(d.called)
        self.assertEqual(d.result,['first','second'])
    
        
        
        
        


        
        
        
        
        


