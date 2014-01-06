import io
try:
    from pulsar.apps.green import green_task, green_loop_thread, GreenProtocol
    from pulsar.utils.pep import to_bytes
    from pulsar import get_event_loop, coroutine_return, in_loop_thread

    class EchoClient(object):
        connection = None

        def __init__(self, address, loop=None):
            self._loop = loop or get_event_loop()
            self.address = address

        @green_loop_thread
        def __call__(self, message):
            self.connection.sendall(to_bytes(message))
            return self.connection.recv(io.DEFAULT_BUFFER_SIZE)

        @in_loop_thread
        def connect(self):
            host, port = self.address
            _, protocol = yield self._loop.create_connection(
                GreenProtocol, host, port)
            self.connection = protocol
            coroutine_return(protocol)

except ImportError:
    green_task = None

from pulsar import send
from pulsar.apps.test import unittest

from examples.echo.manage import server


@unittest.skipUnless(green_task, 'Requires the greenlet package')
class TestGreenlet(unittest.TestCase):
    server_cfg = None

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0')
        cls.server_cfg = yield send('arbiter', 'run', s)
        cls.address = cls.server_cfg.addresses[0]

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    def test_connect(self):
        echo = EchoClient(self.address)
        proto = yield echo.connect()
        self.assertIsInstance(proto, GreenProtocol)
        result = yield echo('ciao')
        self.assertIsInstance(echo.connection, GreenProtocol)
