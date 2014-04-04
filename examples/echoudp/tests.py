import unittest

from pulsar import send, multi_async, new_event_loop, get_application
from pulsar.utils.pep import range
from pulsar.apps.test import dont_run_with_thread, run_on_arbiter

from .manage import server, Echo, EchoUdpServerProtocol


class TestEchoUdpServerThread(unittest.TestCase):
    concurrency = 'thread'
    server_cfg = None

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   concurrency=cls.concurrency)
        cls.server_cfg = yield send('arbiter', 'run', s)
        cls.client = Echo(cls.server_cfg.addresses[0])

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    def sync_client(self):
        return Echo(self.server_cfg.addresses[0], loop=new_event_loop())

    #    TEST THE SERVER APPLICATION
    @run_on_arbiter
    def test_server_on_arbiter(self):
        app = yield get_application(self.__class__.__name__.lower())
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
    def test_ping(self):
        result = yield self.client(b'ciao luca')
        self.assertEqual(result, b'ciao luca')

    def test_large(self):
        '''Echo a 3MB message'''
        msg = b''.join((b'a' for x in range(2**13)))
        result = yield self.client(msg)
        self.assertEqual(result, msg)

    #    TEST SYNCHRONOUS CLIENT
    def test_sync_echo(self):
        echo = self.sync_client()
        self.assertEqual(echo(b'ciao!'), b'ciao!')
        self.assertEqual(echo(b'fooooooooooooo!'),  b'fooooooooooooo!')


@dont_run_with_thread
class TestEchoUdpServerProcess(TestEchoUdpServerThread):
    concurrency = 'process'
