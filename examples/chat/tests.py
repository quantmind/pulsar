'''Tests the websocket middleware in pulsar.apps.ws.'''
import unittest
import asyncio

from pulsar import send
from pulsar.apps import rpc, http, ws
from pulsar.apps.test import dont_run_with_thread
from pulsar.utils.system import json

from examples.chat.manage import server


class Message(ws.WS):

    def __init__(self, loop):
        self.queue = asyncio.Queue(loop=loop)

    def get(self):
        return self.queue.get()

    def on_message(self, websocket, message):
        self.queue.put_nowait(message)


class TestWebChat(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    async def setUpClass(cls):
        s = server(bind='127.0.0.1:0', name=cls.__name__.lower(),
                   concurrency=cls.concurrency)
        cls.app_cfg = await send('arbiter', 'run', s)
        cls.uri = 'http://%s:%s' % cls.app_cfg.addresses[0]
        cls.ws = 'ws://%s:%s/message' % cls.app_cfg.addresses[0]
        cls.rpc = rpc.JsonProxy('%s/rpc' % cls.uri)
        cls.http = http.HttpClient()

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    async def test_home(self):
        response = await self.http.get(self.uri)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'],
                         'text/html; charset=utf-8')

    async def test_handshake(self):
        ws = await self.http.get(self.ws)
        response = ws.handshake
        self.assertEqual(ws.status_code, 101)
        self.assertEqual(ws.headers['upgrade'], 'websocket')
        self.assertEqual(response.connection, ws.connection)
        self.assertTrue(ws.connection)
        #
        # The connection should not be in the connection pool
        pool = self.http.connection_pools.get(ws._request.key)
        self.assertIsInstance(pool, self.http.connection_pool)
        self.assertFalse(ws.connection in pool)

    async def test_rpc(self):
        '''Send a message to the rpc'''
        loop = self.http._loop
        ws = await self.http.get(self.ws, websocket_handler=Message(loop))
        self.assertEqual(ws.status_code, 101)
        ws.write('Hello there!')
        data = await ws.handler.get()
        data = json.loads(data)
        self.assertEqual(data['message'], 'Hello there!')
        result = await self.rpc.message('Hi!')
        self.assertEqual(result, 'OK')
        data = await ws.handler.get()
        data = json.loads(data)
        self.assertEqual(data['message'], 'Hi!')

    async def test_invalid_method(self):
        p = rpc.JsonProxy(self.uri)
        try:
            await p.message('ciao')
        except http.HttpRequestException as e:
            self.assertEqual(e.response.status_code, 405)
        else:
            assert False, '405 not raised'


@dont_run_with_thread
class TestWebChatProcess(TestWebChat):
    concurrency = 'process'
