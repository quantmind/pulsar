import pulsar
from pulsar.utils.httpurl import to_bytes, to_string
from pulsar.apps import socket
from pulsar.apps.test import unittest, run_on_arbiter
        

class EchoServer(socket.SocketServer):
    socket_server_class = pulsar.AsyncSocketServer 
    

class TestPulsarStreams(unittest.TestCase):
    concurrency = 'thread'
    server = None
    @classmethod
    def setUpClass(cls):
        s = EchoServer(name='echoserver', bind='127.0.0.1:0',
                       concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.mid)
        
    def client(self, **kwargs):
        return pulsar.ClientSocket.connect(self.server.address, **kwargs)
        
    @run_on_arbiter
    def testServer(self):
        app = pulsar.get_application('echoserver')
        self.assertTrue(app.address)
        
    def testSyncClient(self):
        client = self.client()
        self.assertEqual(client.remote_address, self.server.address)
        self.assertFalse(client.async)
        self.assertEqual(client.gettimeout(), None)
        self.assertTrue(client.sock)
        client = self.client(timeout=3)
        self.assertFalse(client.async)
        self.assertEqual(client.gettimeout(), 3)
        self.assertEqual(client.execute(b'ciao'), b'ciao')
        self.assertEqual(client.received, 1)
        self.assertEqual(client.execute(b'bla'), b'bla')
        self.assertEqual(client.received, 2)
        
    def testAsyncClient(self):
        client = self.client(timeout=0)
        self.assertEqual(client.remote_address, self.server.address)
        self.assertEqual(client.gettimeout(), 0)
        self.assertTrue(client.async)
        tot_bytes = client.send(b'ciao')
        self.assertEqual(tot_bytes, 4)
        r = client.read()
        yield r
        self.assertEqual(r.result, b'ciao')
        
    
    