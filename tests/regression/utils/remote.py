from time import sleep
from multiprocessing import Pipe, Process

from pulsar import test
from pulsar.utils.async import RemoteProxyServer, Remote,\
                               RemoteServer, RemoteProxy

from .deferred import TestCbk


__all__ = ['TestRemote',
           'TestRemoteOnProcess']


class RemoteX(RemoteServer):
    '''A toy remote server for testing'''
    _stop = False
    loops = 0
    
    def remote_loops(self):
        return self.loops
                    
    def remote_stop(self):
        self._stop = True
    remote_stop.ack = False


class ServerProcess(Process):
    server_class = RemoteX
    
    def __init__(self, connection):
        super(ServerProcess,self).__init__()
        self._connection = connection
        
    def get_proxy(self, connection):
        pass
    
    def make_server(self):
        return self.server_class(self._connection)
    
    def run(self):
        self.server = self.make_server()
        self._run()
    
    def _run(self):
        raise NotImplementedError
    
    
class ServerProcessTest(ServerProcess):
    
    def _run(self):
        server = self.server
        server.loops = 0
        while not self.server._stop:
            server.loops += 1
            server.flush()
            sleep(0.1)            
    
    
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
        
        
class TestRemoteOnProcess(test.TestCase):
    
    def setUp(self):
        a,b = Pipe()
        self.a,self.b = RemoteProxyServer(a),ServerProcessTest(b)
        
    def testSimple(self):
        try:
            server_a,server_b = self.a,self.b
            # lets start the server b
            server_b.start()
            self.sleep(0.2)
            self.assertTrue(server_b.is_alive())
            #
            # Get the proxy of server b
            proxyb = server_b.get_proxy(server_a)
            self.assertEqual(proxyb.proxyid,server_b.proxyid)
            #
            # number of loops in server b
            cbk = TestCbk()
            proxyb.loops().add_callback(cbk)
            server_a.flush()
            loop1 = cbk.result
            self.assertTrue(loop1 > 0)
            
            self.sleep(0.2)
            proxyb.loops().add_callback(cbk)
            server_a.flush()
            loop2 = cbk.result
            self.assertTrue(loop2 > loop1)
            
            # lets kill server b
            proxyb.stop()
            server_a.flush()
            self.sleep(0.2)
            self.assertFalse(server_b.is_alive())
        finally:
            server_b.terminate()
            sleep(0.2)
            self.assertFalse(server_b.is_alive())
        
