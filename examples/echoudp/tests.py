import asyncio
import unittest

from pulsar import send, new_event_loop, get_application
from pulsar.apps.test import dont_run_with_thread

from examples.echoudp.manage import server, Echo, EchoUdpServerProtocol


class TestEchoUdpServerThread(unittest.TestCase):
    concurrency = 'thread'
    server_cfg = None

    @classmethod
    @asyncio.coroutine
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   concurrency=cls.concurrency)
        cls.server_cfg = yield from send('arbiter', 'run', s)
        cls.client = Echo(cls.server_cfg.addresses[0])

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    #    TEST THE SERVER APPLICATION
    @asyncio.coroutine
    def test_server_on_arbiter(self):
        app = yield from get_application(self.__class__.__name__.lower())
        cfg = app.cfg
        self.assertTrue(cfg.addresses)
        self.assertTrue(cfg.address)
        self.assertNotEqual(cfg.addresses[0], cfg.address)

    def test_server(self):
        server = self.server_cfg.app()
        self.assertTrue(server)
        self.assertEqual(server.cfg.callable, EchoUdpServerProtocol)
        self.assertTrue(server.cfg.addresses)

    #    TEST CLIENT INTERACTION
    @asyncio.coroutine
    def test_ping(self):
        result = yield from self.client(b'ciao luca')
        self.assertEqual(result, b'ciao luca')

    @asyncio.coroutine
    def test_large(self):
        '''Echo a 3MB message'''
        msg = b''.join((b'a' for x in range(2**13)))
        result = yield from self.client(msg)
        self.assertEqual(result, msg)


@dont_run_with_thread
class TestEchoUdpServerProcess(TestEchoUdpServerThread):
    concurrency = 'process'

    def sync_client(self):
        return Echo(self.server_cfg.addresses[0], loop=new_event_loop())

    #    TEST SYNCHRONOUS CLIENT
    def test_sync_echo(self):
        echo = self.sync_client()
        self.assertEqual(echo(b'ciao!'), b'ciao!')
        self.assertEqual(echo(b'fooooooooooooo!'),  b'fooooooooooooo!')
