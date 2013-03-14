from functools import partial

import pulsar
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
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
            
    def test_server(self):
        self.assertTrue(self.server)
        self.assertEqual(self.server.callable, EchoServerProtocol)
        self.assertTrue(self.server.address)
        
    def test_ping(self):
        c = Echo(self.server.address)
        response = c.request(b'ciao')
        yield response.on_finished
        self.assertEqual(response.result, b'ciao')
        

@dont_run_with_thread
class TestEchoServerProcess(TestEchoServerThread):
    concurrency = 'process'