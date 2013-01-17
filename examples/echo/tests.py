from functools import partial

import pulsar
from pulsar.apps.test import unittest

from .manage import server, Echo, EchoServerConsumer



class TestEchoServer(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   backlog=1024, concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome.when_ready
        cls.server = outcome.when_ready.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
            
    def test_server(self):
        self.assertTrue(self.server)
        self.assertEqual(self.server.callable, EchoServerConsumer)
        self.assertTrue(self.server.address)
        
    def test_ping(self):
        c = Echo(self.server.address)
        response = c.request(b'ciao')
        yield response.when_ready
        self.assertEqual(response.message, b'ciao')