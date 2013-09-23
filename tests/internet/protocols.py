import socket

import pulsar
from pulsar.utils.pep import to_bytes, to_string
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread

from examples.echo.manage import server, Echo

    
class TestPulsarStreams(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   concurrency=cls.concurrency)
        cls.server = yield pulsar.send('arbiter', 'run', s)
        
    def client(self, **params):
        return Echo(**params)
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
        
    @run_on_arbiter
    def test_server(self):
        app = yield pulsar.get_application(self.__class__.__name__.lower())
        self.assertTrue(app.address)
        self.assertTrue(app.cfg.address)
        self.assertNotEqual(app.address, app.cfg.address)
        
    def test_client_first_request(self):
        client = self.client(full_response=True)
        self.assertFalse(client.concurrent_connections)
        self.assertFalse(client.available_connections)
        response = yield client.request(self.server.address,
            b'Test First request').on_finished
        self.assertEqual(response.buffer, b'Test First request')
        self.assertTrue(response.request)
        self.assertFalse(client.concurrent_connections)
        self.assertEqual(client.available_connections, 1)
        connection = client.get_connection(response.request)
        self.assertEqual(client.concurrent_connections, 1)
        self.assertFalse(client.available_connections)
        self.assertEqual(connection.session, 1)
        self.assertEqual(connection.processed, 1)
        
        
@dont_run_with_thread
class TestPulsarStreamsProcess(TestPulsarStreams):
    impl = 'process'        

    