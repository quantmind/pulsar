'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar import send
from pulsar.apps.ws import WebSocket
from pulsar.apps.http import HttpClient
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server


class TestWebSocketThread(unittest.TestCase):
    app = None
    concurrency = 'thread'
    
    @classmethod
    def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.__name__,
                   concurrency=cls.concurrency)
        cls.app = yield send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.ws_uri = 'ws://{0}:{1}/data'.format(*cls.app.address)
        cls.ws_echo = 'ws://{0}:{1}/echo'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)
    
    def testHyBiKey(self):
        w = WebSocket('/', None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
    def testBadRequests(self):
        c = HttpClient()
        response = yield c.post(self.ws_uri).on_finished
        self.assertEqual(response.status_code, 405)
        #
        response = yield c.get(self.ws_uri,
                        headers=[('Sec-Websocket-Key', '')]).on_finished
        self.assertEqual(response.status_code, 400)
        #
        response = yield c.get(self.ws_uri,
                        headers=[('Sec-Websocket-Key', 'bla')]).on_finished
        self.assertEqual(response.status_code, 400)
        #
        response = yield c.get(self.ws_uri,
                        headers=[('Sec-Websocket-version', 'xxx')]).on_finished
        self.assertEqual(response.status_code, 400)
    
    def testUpgrade(self):
        c = HttpClient()
        response = c.get(self.ws_echo)
        ws = yield response.on_finished
        response = ws.handshake 
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        # Send a message to the websocket
        ws.write('Hi there!')
        

@dont_run_with_thread
class TestWebSocketProcess(TestWebSocketThread):
    concurrency = 'process'
    