from functools import partial

import pulsar
from pulsar import multi_async
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server, Echo, EchoServerProtocol



class TestEchoServerThread(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   backlog=1024, concurrency=cls.concurrency)
        cls.server = yield pulsar.send('arbiter', 'run', s)
        cls.client = Echo(cls.server.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
            
    def test_server(self):
        self.assertTrue(self.server)
        self.assertEqual(self.server.callable, EchoServerProtocol)
        self.assertTrue(self.server.address)
        
    def test_ping(self):
        c = self.client
        result = yield c.request(b'ciao luca')
        self.assertEqual(result, b'ciao luca')
        
    def test_multi(self):
        c = self.client
        result = yield multi_async((c.request(b'ciao'),
                                    c.request(b'pippo'),
                                    c.request(b'foo')))
        self.assertEqual(len(result), 3)
        self.assertTrue(b'ciao' in result)
        self.assertTrue(b'pippo' in result)
        self.assertTrue(b'foo' in result)
        
    def test_client(self):
        c = self.client
        yield self.test_multi()
        self.assertTrue(len(c.connection_pools), 1)
        self.assertTrue(c.available_connections)
        

@dont_run_with_thread
class TestEchoServerProcess(TestEchoServerThread):
    concurrency = 'process'