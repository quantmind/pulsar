from multiprocessing import Pipe

from pulsar import test
from pulsar.utils.async import RemoteProxyServer, Remote,\
                               RemoteServer, RemoteProxy

from .deferred import TestCbk


__all__ = ['TestRemote']

    
class RObject(Remote):
    '''A simple object with two remote functions'''
    
    def __init__(self):
        self.data = []
        
    def remote_notify(self, t):
        self.notified = t
    remote_notify.ack = False
        
    def remote_info(self):
        return {'bla':1}

    def remote_add(self, obj):
        self.data.append(obj)
    remote_add.ack = False


class TestRemote(test.TestCase):
    
    def setUp(self):
        a,b = Pipe()
        self.a,self.b = RemoteProxyServer(a),RemoteProxyServer(b)
        
    def testRemoteSimple(self):
        server_a,server_b = self.a,self.b
        self.assertEqual(len(RObject.remote_functions),3)
        self.assertEqual(len(RObject.remotes),3)
        
        # Create an object and register with server a
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
    
    def testRemoteObjectInFunctionParameters(self):
        '''Test the correct handling of remote object as
parameters of remote functions.'''
        server_a,server_b = self.a,self.b
        objA = RObject().register_with_server(server_a)
        objB = RObject().register_with_server(server_b)
        self.assertNotEqual(objA.proxyid,objB.proxyid)
        #
        # Get the proxy of objA in serber B
        objA_b = objA.get_proxy(server_b)
        self.assertEqual(objA.proxyid,objA_b.proxyid)
        objA_b.add(3)
        objA_b.add('ciao')
        self.assertFalse(objA.data)
        server_a.flush()
        self.assertEqual(objA.data[0],3)
        self.assertEqual(objA.data[1],'ciao')
        objA_b.add([3,4])
        server_a.flush()
        self.assertEqual(objA.data[2],[3,4])
        #
        # Now add remote object B
        objA_b.add(objB)
        server_a.flush()
        rb = objA.data[3]
        self.assertTrue(isinstance(rb,RemoteProxy))
        self.assertEqual(rb.proxyid,objB.proxyid)
        self.assertEqual(rb.connection,server_a.connection)
        
        # Now we have a proxy of object B in server A domain
        rb.notify('Hello')
        server_b.flush()
        self.assertEqual(objB.notified,'Hello')
        
        # Now pass the proxy
        rb.add(rb)
        server_b.flush()
        self.assertEqual(objB.data[0],objB)
        
        

