from functools import partial

import pulsar
from pulsar.apps.test import unittest

from .manage import server



class TestEchoServer(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        server_factory = partial(pulsar.create_server, response=EchoResponse)
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   backlog=1024, concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
            
    def test_server(self):
        self.assertTrue(self.server)