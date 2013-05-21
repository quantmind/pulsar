'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar import send
from pulsar.apps import rpc, http
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server


class TestWebChat(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.__name__,
                   concurrency=cls.concurrency)
        cls.app = yield send('arbiter', 'run', s)
        cls.uri = 'http://%s:%s' % cls.app.address
        cls.ws = 'ws://%s:%s/message' % cls.app.address
        cls.rpc = rpc.JsonProxy('%s/rpc' % cls.uri)
        cls.http = http.HttpClient()
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)
            
    def test_rpc(self):
        '''Send a message to the rpc'''
        response = yield self.http.get(self.ws).on_finished
        self.assertEqual(response.handshake.status_code, 101)
        self.assertFalse(response.request_processed)
        result = yield self.rpc.message('Hi!')
        self.assertEqual(result, 'OK')
        
    def test_handshake(self):
        ws = yield self.http.get(self.ws).on_finished
        response = ws.handshake 
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        self.assertEqual(response.connection, None)
        self.assertTrue(ws.connection)
        