'''Deferred and asynchronous tools.'''
import unittest as test

from pulsar import AlreadyCalledError, Deferred, async_pair


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
        


