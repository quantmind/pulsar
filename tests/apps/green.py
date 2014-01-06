import io
try:
    import greenlet
    from pulsar.apps.green import green_run, green_loop_thread, GreenProtocol
    from pulsar.utils.pep import to_bytes
    from pulsar import get_event_loop, coroutine_return, in_loop_thread

    class EchoClient(object):
        connection = None

        def __init__(self, address, loop=None):
            self._loop = loop or get_event_loop()
            self.address = address

        @green_loop_thread
        def __call__(self, message):
            if not self.connection:
                self.connection = self._connect()
            self.connection.sendall(to_bytes(message)+separator)
            msg = self.connection.recv(io.DEFAULT_BUFFER_SIZE)
            return msg[:-len(separator)]

        @in_loop_thread
        def connect(self):
            host, port = self.address
            _, protocol = yield self._loop.create_connection(
                GreenProtocol, host, port)
            self.connection = protocol
            coroutine_return(protocol)

        @green_run
        def _connect(self):
            host, port = self.address
            _, protocol = yield self._loop.create_connection(
                GreenProtocol, host, port)
            coroutine_return(protocol)


except ImportError:
    greenlet = None

from pulsar import send, async_sleep, maybe_async, new_event_loop
from pulsar.apps.test import unittest

from examples.echo.manage import server, EchoProtocol

separator = EchoProtocol.separator


@unittest.skipUnless(greenlet, 'Requires the greenlet package')
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

    def test_message_async(self):
        echo = EchoClient(self.address)
        result = yield echo('ciao')
        self.assertEqual(result, b'ciao')

    def test_message_sync(self):
        echo = EchoClient(self.address, loop=new_event_loop())
        proto = echo.connect()
        self.assertIsInstance(proto, GreenProtocol)
        result = echo('ciao')
        self.assertEqual(result, b'ciao')

    def test_green_switch(self):
        self._result = None
        result = greenlet.greenlet(self._async_sleep).switch()
        self.assertEqual(self._result, None)
        yield async_sleep(2)
        self.assertEqual(self._result, 2)

    def _async_sleep(self):
        self._result = green_run(async_sleep)(2)
