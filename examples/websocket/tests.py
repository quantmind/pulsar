'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar import send
from pulsar.apps.ws import WebSocket, HttpClient
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server


class TestWebSocketThread(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.__name__,
                   concurrency=cls.concurrency)
        outcome = send('arbiter', 'run', s)
        yield outcome
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.ws_uri = 'ws://{0}:{1}/data'.format(*cls.app.address)
        cls.ws_echo = 'ws://{0}:{1}/echo'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)
    
    def testHyBiKey(self):
        w = WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
    def testBadRequests(self):
        c = HttpClient()
        outcome = c.post(self.ws_uri)
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
        #
        outcome = c.get(self.ws_uri, headers=[('Sec-Websocket-Key', '')])
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
        #
        outcome = c.get(self.ws_uri, headers=[('Sec-Websocket-Key', 'bla')])
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
        #
        outcome = c.get(self.ws_uri, headers=[('Sec-Websocket-version', 'xxx')])
        yield outcome
        response = outcome.result
        self.assertEqual(response.status_code, 400)
    
    def testUpgrade(self):
        c = HttpClient()
        outcome = c.get(self.ws_echo)
        yield outcome
        ws = outcome.result
        response = ws.handshake 
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        # Send a message to the websocket
        outcome = ws.execute('Hi there!')
        yield outcome
        response = outcome.result
        self.assertEqual(response.body, 'Hi there!')
        self.assertFalse(response.masking_key)
        

@dont_run_with_thread
class TestWebSocketProcess(TestWebSocketThread):
    concurrency = 'process'
    