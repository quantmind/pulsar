'''Test twisted integration'''
import pulsar
from pulsar import is_failure, is_async
from pulsar.utils.pep import to_bytes, to_string
from pulsar.utils.security import gen_unique_id
from pulsar.apps.test import unittest, dont_run_with_thread

from examples.echo.manage import server, EchoProtocol

try:
    # This import must be done before importing twisted
    from pulsar.lib.tx import twisted
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
            
        def send_message(self, msg):
            id = gen_unique_id()[:8]
            self.requests[id] = Deferred()
            self.transport.write(to_bytes(id) + to_bytes(msg) + self.separator)
            return self.requests[id]
            
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
    concurrency = 'thread'
    server = None
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
        
    def testEchoClient(self):
        client = get_client(self.server.address)
        self.assertTrue(is_async(client))
        yield client
        client = client.result
        self.assertTrue(client.connected)
        future = client.send_message('Hello')
        yield future
        self.assertEqual(future.result, 'Hello')
        future = client.send_message('Ciao')
        yield future
        self.assertEqual(future.result, 'Ciao')
        