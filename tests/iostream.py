import pulsar
from pulsar.utils.httpurl import to_bytes, to_string
from pulsar.apps.test import unittest


class EchoParser:
    sep = b'\r\n\r\n'
    def encode(self, data):
        return to_bytes(data) + self.sep
    
    def decode(self, data):
        p = data.find(self.sep)
        if p != -1:
            return to_string(data[:p]), data[p+len(self.sep):]
        else:
            return None, data
    

class TestPulsarStreams(unittest.TestCase):
    server = None
    @classmethod
    def setUpClass(cls):
        cls.server = pulsar.AsyncSocketServer.make(
                                bind='127.0.0.1:0',
                                parser_class=EchoParser)
        cls.server.start()
        # RELEASE THE LOOP
        yield pulsar.NOT_DONE
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            cls.server.close()
    
    def setUp(self):
        self.server.quit_connections()
        
    def client(self, **kwargs):
        return pulsar.ClientSocket.connect(self.server.address,
                                           parsercls=EchoParser,
                                           **kwargs)
        
        
    def testServer(self):
        server = self.server
        self.assertTrue(server.address)
        self.assertEqual(server.gettimeout(), 0)
        self.assertEqual(server.active_connections, 0)
        self.assertTrue(server.started)
        
    def __testClient(self):
        client = self.client()
        self.assertEqual(client.remote_address, self.server.address)
        self.assertTrue(client.sock)
        
    def testAsyncClient(self):
        client = self.client(timeout=0)
        self.assertEqual(client.remote_address, self.server.address)
        self.assertEqual(client.gettimeout(), 0)
        self.assertTrue(client.async)
        tot_bytes = client.send('ciao')
        self.assertTrue(tot_bytes>4)
        r = client.read()
        yield r
        self.assertEqual(r.result, 'ciao')
        
    
    