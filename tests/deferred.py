'''Deferred and asynchronous tools.'''
from pulsar import AlreadyCalledError, Deferred, async_pair, is_async
from pulsar.utils.test import test


class Cbk(Deferred):
    
    def __init__(self, r = None):
        super(Cbk,self).__init__()
        self.r = (r,)
        
    def set_result(self, result):
        self.callback(self.r + (result,))
    

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
        e = Exception('blabla exception')
        d,cbk = async_pair(Deferred())
        self.assertFalse(d.called)
        d.callback(e)
        self.assertTrue(d.called)
        self.assertEqual(cbk.result,e)
        
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
        
        
        


