from pulsar import test, AlreadyCalledError
from pulsar.utils.async import Deferred

__all__ = ['TestDeferred']
        
class TestCbk(object):
    
    def __call__(self, result):
        self.result = result
        

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
        cbk = TestCbk()
        d = Deferred().add_callback(cbk)
        self.assertFalse(d.called)
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(cbk.result,'ciao')
                