import unittest

from pulsar.api import send, get_application

from examples.echoudp.manage import server, Echo


class A(unittest.TestLoader):
    # class TestEchoUdpServerThread(unittest.TestCase):
    concurrency = 'process'
    server_cfg = None

    @classmethod
    async def setUpClass(cls):
        s = server(
            name=cls.__name__.lower(),
            bind='127.0.0.1:0',
            concurrency=cls.concurrency,
            parse_console=False
        )
        cls.server_cfg = await send('arbiter', 'run', s)
        cls.client = Echo(cls.server_cfg.addresses[0])

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    #    TEST THE SERVER APPLICATION
    async def test_server_on_arbiter(self):
        app = await get_application(self.__class__.__name__.lower())
        cfg = app.cfg
        self.assertTrue(cfg.addresses)
        self.assertTrue(cfg.address)
        self.assertNotEqual(cfg.addresses[0], cfg.address)

    def test_server(self):
        server = self.server_cfg.app()
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
