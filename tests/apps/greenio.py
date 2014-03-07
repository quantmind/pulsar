import unittest

try:
    from pulsar.apps import greenio
except ImportError:
    greenio = None

from pulsar import send, multi_async

from examples.echo.manage import server, Echo


@unittest.skipUnless(greenio, 'Requires the greenlet package')
class TestGreenIO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0')
        cls.server_cfg = yield send('arbiter', 'run', s)
        cls.client = Echo(cls.server_cfg.addresses[0],
                          loop=greenio.GreenEventLoop())

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    def test_loop(self):
        c = self.client
        self.assertIsInstance(c._loop, greenio.GreenEventLoop)
        self.assertTrue(c._loop._loop)

    def test_ping(self):
        result = yield self.client(b'ciao luca')
        self.assertEqual(result, b'ciao luca')

    def test_large(self):
        '''Echo a 3MB message'''
        msg = b''.join((b'a' for x in range(2**13)))
        result = yield self.client(msg)
        self.assertEqual(result, msg)

    def test_multi(self):
        result = yield multi_async((self.client(b'ciao'),
                                    self.client(b'pippo'),
                                    self.client(b'foo')))
        self.assertEqual(len(result), 3)
        self.assertTrue(b'ciao' in result)
        self.assertTrue(b'pippo' in result)
        self.assertTrue(b'foo' in result)
