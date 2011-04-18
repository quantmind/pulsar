from multiprocessing import Pipe

from pulsar import test
from pulsar.utils.defer import RemoteProxy

from .deferred import TestCbk


__all__ = ['TestRemoteProxy']


class RemoteProxyA(RemoteProxy):
    remotes = ('notify','info')
    
    def proxy_info(self, response):
        result = response.result
        
    
    
class RemoteProxyB(RemoteProxy):
    
    def proxy_notify(self, t):
        self.notified = t
        
    def proxy_info(self):
        return {'bla':1}
    

class TestRemoteProxy(test.TestCase):
    
    def setUp(self):
        a,b = Pipe()
        self.a,self.b = RemoteProxyA(a),RemoteProxyB(b)
        
    def testRemoteSimple(self):
        a,b = self.a,self.b
        self.assertEqual(len(a.proxy_functions),1)
        self.assertEqual(len(b.proxy_functions),2)
        a.notify('ciao')
        b.flush()
        self.assertEqual(b.notified,'ciao')
        a.notify('bla')
        self.assertEqual(b.notified,'ciao')
        b.flush()
        
    def testRemoteCallBack(self):
        cbk = TestCbk()
        a,b = self.a,self.b
        request = a.info().add_callback(cbk)
        self.assertTrue(request.rid)
        self.assertEqual(len(request._callbacks),1)
        b.flush()
        a.flush()
        self.assertEqual(cbk.result,{'bla':1})
        