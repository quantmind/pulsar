'''Test twisted integration'''
import unittest

import pulsar
from pulsar import multi_async
from pulsar.utils.pep import to_bytes, to_string
from pulsar.utils.security import gen_unique_id

from examples.echo.manage import server, EchoProtocol

try:
    # This import must be done before importing twisted
    from pulsar.apps.tx import twisted
    from twisted.internet.protocol import Factory, Protocol
    from twisted.internet.defer import Deferred
    from twisted.internet.endpoints import TCP4ClientEndpoint
    from twisted.internet import reactor

    class EchoClient(Protocol):
        '''Twisted client to the Echo server in the examples.echo module'''
        separator = EchoProtocol.separator
        connected = False

        def __init__(self):
            self.buffer = b''
            self.requests = {}

        def connectionMade(self):
            self.connected = True

        def __call__(self, msg):
            id = to_bytes(gen_unique_id()[:8])
            self.requests[id] = d = Deferred()
            self.transport.write(id + to_bytes(msg) + self.separator)
            return d

        def dataReceived(self, data):
            sep = self.separator
            idx = data.find(sep)
            if idx >= 0:
                if self.buffer:
                    msg = self.buffer + msg
                    self.buffer = b''
                id, msg, data = data[:8], data[8:idx], data[idx+len(sep):]
                d = self.requests.pop(id)
                d.callback(to_string(msg))
                if data:
                    self.dataReceived(data)
            else:
                self.buffer += data

    class EchoClientFactory(Factory):
        protocol = EchoClient

    def get_client(address):
        point = TCP4ClientEndpoint(reactor, *address)
        return point.connect(EchoClientFactory())

except ImportError:
    twisted = None


@unittest.skipUnless(twisted, 'Requires twisted')
class TestTwistedIntegration(unittest.TestCase):
    server_cfg = None

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0')
        cls.server_cfg = yield pulsar.send('arbiter', 'run', s)
        cls.address = cls.server_cfg.addresses[0]

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return pulsar.send('arbiter', 'kill_actor', cls.server_cfg.name)

    def test_echo_client(self):
        client = yield get_client(self.address)
        self.assertTrue(client.connected)
        result = yield client('Hello')
        self.assertEqual(result, 'Hello')
        result = yield client('Ciao')
        self.assertEqual(result, 'Ciao')

    def test_multi_requests(self):
        client = yield get_client(self.address)
        results = yield multi_async((client('Msg%s' % n) for n in range(20)))
        self.assertEqual(len(results), 20)
        for n, result in enumerate(results):
            self.assertEqual(result, 'Msg%s' % n)


@unittest.skipUnless(twisted, 'Requires twisted')
class TestPulsarReactor(unittest.TestCase):

    def test_meta(self):
        self.assertTrue(reactor.running)
        self.assertEqual(reactor.threadpool, None)
        self.assertEqual(reactor.waker, None)

    def test_switched_off_methods(self):
        self.assertRaises(NotImplementedError, reactor.spawnProcess)
