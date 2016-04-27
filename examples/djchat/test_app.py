"""Tests django chat application"""
import os
import sys
import unittest
import asyncio

from pulsar import send, get_application
from pulsar.apps import http, ws
from pulsar.apps.test import dont_run_with_thread, test_timeout
from pulsar.utils.string import gen_unique_id

try:
    from examples.djchat.manage import server
    path = os.path.dirname(__file__)
    if path not in sys.path:
        sys.path.append(path)
except ImportError:
    server = None


async def start_server(actor, name, argv):
    server(argv)
    await asyncio.sleep(0.5)
    app = await get_application(name)
    return app.cfg


class MessageHandler(ws.WS):

    def __init__(self, loop):
        self.queue = asyncio.Queue(loop=loop)

    def get(self):
        return self.queue.get()

    def on_message(self, websocket, message):
        return self.queue.put(message)


@test_timeout(30)
@unittest.skipUnless(server, 'Requires django')
class TestDjangoChat(unittest.TestCase):
    concurrency = 'thread'
    app_cfg = None

    @classmethod
    async def setUpClass(cls):
        cls.exc_id = gen_unique_id()[:8]
        name = cls.__name__.lower()
        argv = [__file__, 'pulse',
                '-b', '127.0.0.1:0',
                '--concurrency', cls.concurrency,
                '--exc-id', cls.exc_id,
                '--pulse-app-name', name,
                '--data-store', 'pulsar://127.0.0.1:6410/1']
        cls.app_cfg = await send('arbiter', 'run', start_server, name, argv)
        assert cls.app_cfg.exc_id == cls.exc_id, "Bad execution id"
        addr = cls.app_cfg.addresses[0]
        cls.uri = 'http://{0}:{1}'.format(*addr)
        cls.ws = 'ws://{0}:{1}/message'.format(*addr)
        cls.http = http.HttpClient()

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    async def test_home(self):
        result = await self.http.get(self.uri)
        self.assertEqual(result.status_code, 200)

    async def test_404(self):
        result = await self.http.get('%s/bsjdhcbjsdh' % self.uri)
        self.assertEqual(result.status_code, 404)

    async def test_websocket(self):
        # TODO: fix this test. Someties it timesout
        c = self.http
        ws = await c.get(self.ws, websocket_handler=MessageHandler(c._loop))
        response = ws.handshake
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        self.assertEqual(response.connection, ws.connection)
        self.assertTrue(ws.connection)
        self.assertIsInstance(ws.handler, MessageHandler)


@dont_run_with_thread
class TestDjangoChat_Process(TestDjangoChat):
    concurrency = 'process'
