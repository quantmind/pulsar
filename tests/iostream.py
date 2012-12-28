import socket

import pulsar
from pulsar import is_failure
from pulsar.utils.httpurl import to_bytes, to_string
from pulsar.apps.socket import SocketServer
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread
        
        
class EchoResponse(pulsar.AsyncResponse):
    
    def __iter__(self):
        if self.parsed_data == b'quit':
            yield b'bye'
            self.connection.close()
        else:
            yield self.parsed_data
            
            
class TestServerSocketServer(pulsar.AsyncSocketServer):
    response_class = EchoResponse
    
    
class SafeCallback(pulsar.Deferred):
    
    def __call__(self):
        try:
            r = self._call()
        except Exception as e:
            r = e
        if pulsar.is_async(r):
            return r.add_callback(self)
        else:
            return self.callback(r)
        
    def _call(self):
        raise NotImplementedError()
    

class TestPulsarStreams(unittest.TestCase):
    concurrency = 'thread'
    server = None
    @classmethod
    def setUpClass(cls):
        s = SocketServer(socket_server_class=TestServerSocketServer,
                         name=cls.__name__.lower(), bind='127.0.0.1:0',
                         concurrency=cls.concurrency)
        outcome = pulsar.send('arbiter', 'run', s)
        yield outcome
        cls.server = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
        
    def client(self, **kwargs):
        return pulsar.ClientSocket.connect(self.server.address, **kwargs)
        
    @run_on_arbiter
    def testServer(self):
        app = pulsar.get_application(self.__class__.__name__.lower())
        self.assertTrue(app.address)
        
    def testSyncClient(self):
        client = self.client()
        self.assertEqual(client.remote_address, self.server.address)
        self.assertFalse(client.async)
        self.assertEqual(client.gettimeout(), None)
        self.assertTrue(client.sock)
        client = self.client(timeout=3)
        self.assertFalse(client.async)
        self.assertEqual(client.gettimeout(), 3)
        self.assertEqual(client.read_timeout, 3)
        client.read_timeout = 4
        self.assertEqual(client.gettimeout(), 4)
        self.assertEqual(client.read_timeout, 4)
        self.assertEqual(client.execute(b'ciao'), b'ciao')
        self.assertEqual(client.received, 1)
        self.assertEqual(client.execute(b'bla'), b'bla')
        self.assertEqual(client.received, 2)
        
    def testAsyncClient(self):
        client = self.client(timeout=0)
        self.assertEqual(client.remote_address, self.server.address)
        self.assertEqual(client.gettimeout(), 0)
        self.assertTrue(client.async)
        tot_bytes = client.send(b'ciao')
        self.assertEqual(tot_bytes, 4)
        r = client.read()
        yield r
        self.assertEqual(r.result, b'ciao')
        
    def test_for_coverage(self):
        io = pulsar.AsyncIOStream()
        self.assertEqual(str(io), '(closed)')
        self.assertEqual(io.read(), b'')
        self.assertEqual(io.state, None)
        self.assertEqual(io.state_code, 'closed')
        conn, sock = pulsar.server_client_sockets()
        io.sock = sock
        io.settimeout(10)
        self.assertEqual(io.gettimeout(), 0)
        def _():
            io.sock = sock
        self.assertRaises(RuntimeError, _)
        
    def test_bad_connect(self):
        io = pulsar.AsyncIOStream()
        io.connect(('bla', 6777))
        self.assertTrue(io.closed)
        self.assertTrue(io.error)
        self.assertTrue(isinstance(io.error, socket.gaierror))
        
    def test_already_connecting(self):
        io = pulsar.AsyncIOStream()
        class _test(SafeCallback):
            def _call(_):
                io.connect(self.server.address)
                self.assertTrue(io.connecting)
                self.assertEqual(io.state_code, 'connecting')
                self.assertRaises(RuntimeError, io.connect, self.server.address)
        cbk = _test()
        # we need to run this test on the ioloop thread
        io.ioloop.add_callback(cbk)
        yield cbk
        
    def test_already_reading(self):
        io = pulsar.AsyncIOStream()
        io.connect(self.server.address)
        while io.connecting:
            yield pulsar.NOT_DONE
        self.assertFalse(io.connecting)
        class _test(SafeCallback):
            def _call(_):
                io.read()
                self.assertTrue(io.reading)
                self.assertEqual(io.state_code, 'reading')
                self.assertRaises(RuntimeError, io.read)
        cbk = _test()
        # we need to run this test on the ioloop thread
        io.ioloop.add_callback(cbk)
        
    def testReadTimeout(self):
        client = self.client(timeout=0)
        self.assertEqual(client.read_timeout, None)
        client.read_timeout = 20
        self.assertEqual(client.read_timeout, 20)
        r = client.execute(b'ciao')
        yield r
        self.assertEqual(r.result, b'ciao')
        self.assertTrue(client.sock._read_timeout)
        # Remove the read_timeout
        client.sock.ioloop.remove_timeout(client.sock._read_timeout)
        r = client.execute(b'pippo')
        yield r
        self.assertEqual(r.result, b'pippo')
        
    def testMaxBufferSize(self):
        client = self.client(timeout=0)
        client.sock.max_buffer_size = 10
        msg = b'this will overflow the reading buffer'
        r = client.execute(msg)
        yield r
        self.assertEqual(r.result, msg)
        self.assertTrue(client.closed)
    
    def testConnectionClose(self):
        client = self.client(timeout=0, read_timeout=3)
        self.assertTrue(client.async)
        self.assertEqual(client.read_timeout, 3)
        future = client.execute(b'ciao')
        yield future
        self.assertEqual(future.result, b'ciao')
        future = client.execute(b'quit')
        yield future
        self.assertEqual(future.result, b'bye')
        try:
            future = client.execute(b'ciao')
        except IOError:
            pass
        else:
            future.add_errback(lambda res: (res,))
            yield future
            result = future.result[0]
            self.assertTrue(is_failure(result))
        
        
@dont_run_with_thread
class TestPulsarStreamsProcess(TestPulsarStreams):
    impl = 'process'        

    