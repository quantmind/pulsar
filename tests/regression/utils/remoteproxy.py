from multiprocessing import Pipe

from pulsar import test
from pulsar.utils.defer import RemoteProxyServer, Remote, RemoteServer

from .deferred import TestCbk


__all__ = ['TestRemoteProxy']

    
class RObject(Remote):
    '''A simple object with two remote functions'''
    def remote_notify(self, t):
        self.notified = t
    remote_notify.ack = False
        
    def remote_info(self):
        return {'bla':1}



class TestRemoteProxy(test.TestCase):
    
    def setUp(self):
        a,b = Pipe()
        self.a,self.b = RemoteProxyServer(a),RemoteProxyServer(b)
        
    def testRemoteSimple(self):
        server_a,server_b = self.a,self.b
        self.assertEqual(len(RObject.remote_functions),2)
        self.assertEqual(len(RObject.remotes),2)
        # CReate an object and register with server a
        objA = RObject().register_with_server(server_a)
        self.assertTrue(objA.proxyid)
        
        # Get a proxy in server b
        pb = objA.get_proxy(server_b)
        self.assertEqual(pb.remotes,objA.remotes)
        self.assertEqual(pb.connection,server_b.connection)
        
        pb.notify('ciao')
        server_a.flush()
        self.assertEqual(objA.notified,'ciao')
        
        self.assertRaises(AttributeError,lambda : pb.blabla)
        
    def testRemotecallback(self):
        server_a,server_b = self.a,self.b
        objA = RObject().register_with_server(server_a)
        pb = objA.get_proxy(server_b)
        
        cbk = TestCbk()
        d = pb.info().add_callback(cbk)
        self.assertFalse(d.called)
        self.assertTrue(len(d._callbacks),1)
        server_a.flush()
        self.assertFalse(d.called)
        server_b.flush()
        self.assertTrue(d.called)
        self.assertEqual(cbk.result,{'bla':1})
        


