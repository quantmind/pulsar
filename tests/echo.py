from functools import partial

import pulsar
from pulsar.apps.test import unittest
from pulsar.apps.socket import SocketServer

class EchoResponse(pulsar.ProtocolResponse):
    
    def __init__(self, protocol, separator='\r\n'):
        super(EchoResponse, self).__init__(protocol)
        self.buffer = bytearray()
        self.separator = separator
        
    def feed(self, data):
        idx = buffer.find(separator)
        if idx < 0:
            self.buffer.extend(buffer)
        else:
            idx += len(self.separator)
            self.buffer.extend(data[:idx])
            self.write(bytes(self.buffer))
            self.buffer = None
            return data[idx:]
    
    def finished(self):
        return self.buffer is None


class TestEchoServer(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        server_factory = partial(pulsar.create_server, response=EchoResponse)
        s = SocketServer(socket_server_factory=server_factory,
                         name=cls.__name__.lower(), bind='127.0.0.1:0',
                         backlog=1024, concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
            
    def test_server(self):
        self.assertTrue(self.server)