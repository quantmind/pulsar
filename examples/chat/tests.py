'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar import send, Queue
from pulsar.apps import rpc, http, ws
from pulsar.apps.test import unittest, dont_run_with_thread
from pulsar.utils.httpurl import HTTPError
from pulsar.utils.system import json

from .manage import server


class MessageHandler(ws.WS):

    def __init__(self):
        self.queue = Queue()

    def get(self):
        return self.queue.get()

    def on_message(self, websocket, message):
        self.queue.put(message)


class TestWebChat(unittest.TestCase):
    app = None
    concurrency = 'thread'

    @classmethod
    def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.__name__.lower(),
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

    def test_home(self):
        response = yield self.http.get(self.uri).on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'text/html')

    def test_rpc(self):
        '''Send a message to the rpc'''
        ws = yield self.http.get(self.ws,
                                 websocket_handler=MessageHandler()
                                 ).on_headers
        self.assertEqual(ws.handshake.status_code, 101)
        ws.write('Hello there!')
        data = yield ws.handler.get()
        data = json.loads(data)
        self.assertEqual(data['message'], 'Hello there!')
        result = yield self.rpc.message('Hi!')
        self.assertEqual(result, 'OK')
        data = yield ws.handler.get()
        data = json.loads(data)
        self.assertEqual(data['message'], 'Hi!')

    def test_handshake(self):
        ws = yield self.http.get(self.ws).on_headers
        response = ws.handshake
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        self.assertEqual(response.connection, ws.connection)
        self.assertTrue(ws.connection)

    def test_invalid_method(self):
        p = rpc.JsonProxy(self.uri)
        try:
            yield p.message('ciao')
        except HTTPError as e:
            self.assertEqual(e.code, 405)
        else:
            assert False, '405 not raised'


@dont_run_with_thread
class TestWebChatProcess(TestWebChat):
    concurrency = 'process'
