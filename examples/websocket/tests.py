'''Tests the websocket middleware in pulsar.apps.ws.'''
import unittest
import asyncio

from pulsar import send, new_event_loop, HAS_C_EXTENSIONS
from pulsar.apps.ws import WebSocket, WS
from pulsar.apps.http import HttpClient
from pulsar.apps.test import dont_run_with_thread

from examples.websocket.manage import server, frame_parser


class Echo(WS):

    def __init__(self, loop):
        self.queue = asyncio.Queue(loop=loop)

    def get(self):
        return self.queue.get()

    def on_message(self, ws, message):
        self.queue.put_nowait(message)

    def on_ping(self, ws, body):
        ws.pong(body)
        self.queue.put_nowait('PING: %s' % body.decode('utf-8'))

    def on_pong(self, ws, body):
        self.queue.put_nowait('PONG: %s' % body.decode('utf-8'))

    def on_close(self, ws):
        self.queue.put_nowait('CLOSE')


class TestWebSocketThread(unittest.TestCase):
    pyparser = False
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    @asyncio.coroutine
    def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.__name__,
                   concurrency=cls.concurrency, pyparser=cls.pyparser)
        cls.app_cfg = yield from send('arbiter', 'run', s)
        addr = cls.app_cfg.addresses[0]
        cls.uri = 'http://{0}:{1}'.format(*addr)
        cls.ws_uri = 'ws://{0}:{1}/data'.format(*addr)
        cls.ws_echo = 'ws://{0}:{1}/echo'.format(*addr)

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    def _frame_parser(self, **params):
        params['pyparser'] = self.pyparser
        return frame_parser(**params)

    def http(self, **params):
        params['frame_parser'] = self._frame_parser
        return HttpClient(**params)

    def test_hybikey(self):
        w = WebSocket('/', None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")

    @asyncio.coroutine
    def test_bad_requests(self):
        c = self.http()
        response = yield from c.post(self.ws_uri)
        self.assertEqual(response.status_code, 405)
        #
        response = yield from c.get(self.ws_uri,
                                    headers=[('Sec-Websocket-Key', 'x')])
        self.assertEqual(response.status_code, 400)
        #
        response = yield from c.get(self.ws_uri,
                                    headers=[('Sec-Websocket-Key', 'bla')])
        self.assertEqual(response.status_code, 400)
        #
        response = yield from c.get(self.ws_uri,
                                    headers=[('Sec-Websocket-version', 'xxx')])
        self.assertEqual(response.status_code, 400)

    @asyncio.coroutine
    def test_upgrade(self):
        c = self.http()
        handler = Echo(c._loop)
        ws = yield from c.get(self.ws_echo, websocket_handler=handler)
        response = ws.handshake
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        self.assertEqual(ws.connection, response.connection)
        self.assertEqual(ws.handler, handler)
        #
        # on_finished
        self.assertTrue(response.on_finished.done())
        self.assertFalse(ws.on_finished.done())
        # Send a message to the websocket
        ws.write('Hi there!')
        message = yield from handler.get()
        self.assertEqual(message, 'Hi there!')

    @asyncio.coroutine
    def test_ping(self):
        c = self.http()
        handler = Echo(c._loop)
        ws = yield from c.get(self.ws_echo, websocket_handler=handler)
        #
        # ASK THE SERVER TO SEND A PING FRAME
        ws.write('send ping TESTING PING')
        message = yield from handler.get()
        self.assertEqual(message, 'PING: TESTING PING')

    @asyncio.coroutine
    def test_pong(self):
        c = self.http()
        handler = Echo(c._loop)
        ws = yield from c.get(self.ws_echo, websocket_handler=handler)
        #
        ws.ping('TESTING CLIENT PING')
        message = yield from handler.get()
        self.assertEqual(message, 'PONG: TESTING CLIENT PING')

    @asyncio.coroutine
    def test_close(self):
        c = self.http()
        handler = Echo(c._loop)
        ws = yield from c.get(self.ws_echo, websocket_handler=handler)
        self.assertEqual(ws.event('post_request').fired(), 0)
        ws.write('send close 1001')
        message = yield from handler.get()
        self.assertEqual(message, 'CLOSE')
        self.assertTrue(ws.close_reason)
        self.assertEqual(ws.close_reason[0], 1001)
        self.assertTrue(ws._connection.closed)

    @asyncio.coroutine
    def test_home(self):
        c = self.http()
        response = yield from c.get(self.uri)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'],
                         'text/html; charset=utf-8')

    @asyncio.coroutine
    def test_graph(self):
        c = self.http()
        handler = Echo(c._loop)
        ws = yield from c.get(self.ws_uri, websocket_handler=handler)
        self.assertEqual(ws.event('post_request').fired(), 0)
        message = yield from handler.get()
        self.assertTrue(message)


@unittest.skipUnless(HAS_C_EXTENSIONS, "Requires C extensions")
class TestWebSocketPyParser(TestWebSocketThread):
    pyparser = True
    concurrency = 'process'


@dont_run_with_thread
class TestWebSocketProcess(TestWebSocketThread):
    concurrency = 'process'

    def __test_close_sync(self):
        loop = new_event_loop()
        c = self.http(loop=loop)
        handler = Echo(loop)
        ws = c.get(self.ws_echo, websocket_handler=handler)
        self.assertEqual(ws.event('post_request').fired(), 0)
        self.assertEqual(ws._loop, loop)
        self.assertFalse(ws._loop.is_running())
        ws.write('send close 1001')
        message = ws._loop.run_until_complete(handler.get())
        self.assertEqual(message, 'CLOSE')
        self.assertTrue(ws.close_reason)
        self.assertEqual(ws.close_reason[0], 1001)
        self.assertTrue(ws._connection.closed)
