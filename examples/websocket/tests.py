'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar import send, Queue
from pulsar.apps.ws import WebSocket, WS
from pulsar.apps.http import HttpClient
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server


class Echo(WS):

    def __init__(self):
        self.queue = Queue()

    def get(self):
        return self.queue.get()

    def on_message(self, ws, message):
        return self.queue.put(message)

    def on_ping(self, ws, body):
        super(Echo, self).on_ping(ws, body)
        return self.queue.put('PING: %s' % body.decode('utf-8'))

    def on_pong(self, ws, body):
        return self.queue.put('PONG: %s' % body.decode('utf-8'))


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
                               headers=[('Sec-Websocket-Key', '')]
                               ).on_finished
        self.assertEqual(response.status_code, 400)
        #
        response = yield c.get(self.ws_uri,
                               headers=[('Sec-Websocket-Key', 'bla')]
                               ).on_finished
        self.assertEqual(response.status_code, 400)
        #
        response = yield c.get(self.ws_uri,
                               headers=[('Sec-Websocket-version', 'xxx')]
                               ).on_finished
        self.assertEqual(response.status_code, 400)

    def test_upgrade(self):
        c = HttpClient()
        handler = Echo()
        ws = yield c.get(self.ws_echo, websocket_handler=handler).on_headers
        response = ws.handshake
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        self.assertEqual(ws.connection, response.connection)
        self.assertEqual(ws.handler, handler)
        #
        # on_finished
        self.assertFalse(response.on_finished.done())
        self.assertFalse(ws.on_finished.done())
        # Send a message to the websocket
        ws.write('Hi there!')
        message = yield handler.get()
        self.assertEqual(message, 'Hi there!')

    def test_ping(self):
        c = HttpClient()
        handler = Echo()
        ws = yield c.get(self.ws_echo, websocket_handler=handler).on_headers
        #
        # ASK THE SERVER TO SEND A PING FRAME
        ws.write('send ping TESTING PING')
        message = yield handler.get()
        self.assertEqual(message, 'PING: TESTING PING')

    def test_pong(self):
        c = HttpClient()
        handler = Echo()
        ws = yield c.get(self.ws_echo, websocket_handler=handler).on_headers
        #
        ws.ping('TESTING CLIENT PING')
        message = yield handler.get()
        self.assertEqual(message, 'PONG: TESTING CLIENT PING')


@dont_run_with_thread
class TestWebSocketProcess(TestWebSocketThread):
    concurrency = 'process'
