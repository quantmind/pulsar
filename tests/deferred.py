'''Deferred and asynchronous tools.'''
import sys

from pulsar import AlreadyCalledError, Deferred, async_pair, is_async,\
                     make_async, IOLoop, is_failure
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


class TestDeferred(test.TestCase):
    
    def testSimple(self):
        d = Deferred()
        self.assertEqual(d.rid,None)
        self.assertFalse(d.called)
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.result,'ciao')
        self.assertRaises(AlreadyCalledError,d.callback,'bla')

    def testCallbacks(self):
        d,cbk = async_pair(Deferred())
        self.assertFalse(d.called)
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(cbk.result,'ciao')
        
    def testError(self):
        d,cbk = async_pair(Deferred())
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
        
        
        
        
        
        


        
        
        
        
        


