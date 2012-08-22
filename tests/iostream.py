import pulsar
from pulsar.apps.test import unittest


class TestPulsarStreams(unittest.TestCase):
    server = None
    @classmethod
    def setUpClass(cls):
        cls.server = pulsar.AsyncSocketServer.make(bind='127.0.0.1:0')
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            cls.server.close()
    
    def setUp(self):
        self.server.quit_connections()
        
    def testServer(self):
        server = self.server
        self.assertTrue(server.address)
        self.assertEqual(server.socket.timeout, 0)
        self.assertEqual(server.active_connections, 0)
        
    def testClient(self):
        client = pulsar.ClientSocket.connect(self.server.address)
        self.assertEqual(client.remote_address, self.server.address)
        self.assertTrue(client.socket)
    
    