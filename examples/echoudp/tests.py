import unittest

from pulsar.api import send, get_application
from pulsar.apps.test import run_test_server

from examples.echoudp.manage import server, Echo


class A(unittest.TestLoader):
    # class TestEchoUdpServerThread(unittest.TestCase):
    concurrency = 'process'
    app_cfg = None

    @classmethod
    async def setUpClass(cls):
        await run_test_server(cls, server)
        cls.client = Echo(cls.app_cfg.addresses[0])

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    #    TEST THE SERVER APPLICATION
    async def test_server_on_arbiter(self):
        app = await get_application(self.app_cfg.name)
        cfg = app.cfg
        self.assertTrue(cfg.addresses)
        self.assertTrue(cfg.address)
        self.assertNotEqual(cfg.addresses[0], cfg.address)

    def test_server(self):
        server = self.app_cfg.app()
        self.assertTrue(server)
        self.assertTrue(server.cfg.addresses)

    #    TEST CLIENT INTERACTION
    async def test_ping(self):
        result = await self.client(b'ciao luca')
        self.assertEqual(result, b'ciao luca')

    async def test_large(self):
        '''Echo a 3MB message'''
        msg = b''.join((b'a' for x in range(2**13)))
        result = await self.client(msg)
        self.assertEqual(result, msg)
