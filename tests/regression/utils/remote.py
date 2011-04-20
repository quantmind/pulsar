from time import sleep
from multiprocessing import Pipe, Process
from threading import Thread

from pulsar import test
from pulsar.utils.async import RemoteServer, Remote,\
                               ProcessWithRemote, RemoteController,\
                               RemoteProxy

from .deferred import TestCbk


__all__ = ['TestRemote',
           'TestRemoteOnProcess']


class RemoteX(RemoteController):
    '''A toy remote controller for testing'''
    loops = 0
    def remote_loops(self):
        return self.loops


class ServerTest(ProcessWithRemote):
    controller_class = RemoteX
    _cont = True
        
    def _run(self):
        server = self.server
        while self._cont:
            server.loops += 1
            server.flush()
            sleep(0.05)
    
    def _stop(self):
        self._cont = False   
    

class ServerProcessTest(ServerTest,Process):
    
    def __init__(self, *args):
        super(ServerProcessTest,self).__init__(*args)
        Process.__init__(self)
        

class ServerThreadTest(ServerTest,Thread):
    
    def __init__(self, *args):
        super(ServerThreadTest,self).__init__(*args)
        Thread.__init__(self)

    
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
        self.a,self.b = RemoteServer(a),RemoteServer(b)
        
    def testRemoteSimple(self):
        server_a,server_b = self.a,self.b
        self.assertEqual(len(RObject.remote_functions),6)
        self.assertEqual(len(RObject.remotes),6)
        
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
        
    def testRegisterRemote(self):
        server_a,server_b = self.a,self.b
        server_a.register_with_remote()
        self.assertEqual(server_a.remote,None)
        server_b.flush()
        self.assertEqual(server_a.remote,None)
        # But server_b has received server_a
        self.assertTrue(server_b.remote)
        self.assertEqual(server_a.proxyid,server_b.remote.proxyid)
        server_a.flush()
        self.assertTrue(server_a.remote)
        self.assertEqual(server_a.remote.proxyid,server_b.proxyid)
        
class TestRemoteOnProcess(test.TestCase):
    
    def setUp(self):
        a,b = Pipe()
        self.a,self.b = RemoteServer(a,log=self.log),b
        
    def __testConnectorInThread(self):
        server_b = ServerThreadTest(self.b)
        self._connector_test(server_b)
        
    def _testConnectorInProcess(self):
        server_b = ServerProcessTest(self.b)
        self._connector_test(server_b)
        
    def _connector_test(self, server_b):
        server_a = self.a
        # lets start the server b
        server_b.start()
        self.sleep(0.2)
        self.assertTrue(server_b.is_alive())
        #
        # number of loops in server b
        server_a.register_with_remote()
        self.assertEqual(server_a.remote,None)
        self.sleep(1) # make sure we receive the callback
        server_a.flush()
        self.assertTrue(server_a.remote)
        
        cbk = TestCbk()
        server_a.remote.loops().add_callback(cbk)
        server_a.flush()
        self.sleep(0.2)
        loop1 = cbk.result
        self.assertTrue(loop1 > 0)
        
        self.sleep(0.2)
        server_a.remote.loops().add_callback(cbk)
        server_a.flush()
        self.sleep(0.2)
        loop2 = cbk.result
        self.assertTrue(loop2 > loop1)
        
        # lets kill server b
        server_a.remote.stop()
        server_a.flush()
        self.sleep(0.2)
        self.assertFalse(server_b.is_alive())
