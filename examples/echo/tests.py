from pulsar import send, multi_async
from pulsar.utils.pep import range
from pulsar.apps.test import unittest, dont_run_with_thread

from .manage import server, Echo, EchoServerProtocol


class TestEchoServerThread(unittest.TestCase):
    concurrency = 'thread'
    server = None

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   backlog=1024, concurrency=cls.concurrency)
        cls.server = yield send('arbiter', 'run', s)
        cls.client = Echo(cls.server.address)

    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield send('arbiter', 'kill_actor', cls.server.name)

    def test_server(self):
        self.assertTrue(self.server)
        self.assertEqual(self.server.callable, EchoServerProtocol)
        self.assertTrue(self.server.address)

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

    # TESTS FOR PROTOCOLS AND CONNECTIONS
    def test_client(self):
        yield self.test_multi()
        c = self.client
        self.assertTrue(c.pool.available)

    def test_info(self):
        info = yield send(self.server.name, 'info')
        self.assertIsInstance(info, dict)
        self.assertEqual(info['actor']['name'], self.server.name)
        self.assertEqual(info['actor']['concurrency'], self.concurrency)

    def test_connection(self):
        client = Echo(self.server.address, full_response=True)
        response = yield client(b'test connection')
        self.assertEqual(response.buffer, b'test connection')
        connection = response.connection
        self.assertTrue(str(connection))
        self.assertEqual(str(connection.transport)[:4], 'TCP ')

    def test_connection_pool(self):
        client = Echo(self.server.address, pool_size=2)
        self.assertEqual(client.pool.pool_size, 2)
        self.assertEqual(client.pool.in_use, 0)
        self.assertEqual(client.pool.available, 0)
        self.assertEqual(client.sessions, 0)
        self.assertEqual(client._requests_processed, 0)
        #
        response = yield client(b'test connection')
        self.assertEqual(response, b'test connection')
        self.assertEqual(client.pool.in_use, 0)
        self.assertEqual(client.pool.available, 1)
        self.assertEqual(client.sessions, 1)
        self.assertEqual(client._requests_processed, 1)
        #
        response = yield client(b'test connection 2')
        self.assertEqual(response, b'test connection 2')
        self.assertEqual(client.pool.in_use, 0)
        self.assertEqual(client.pool.available, 1)
        self.assertEqual(client.sessions, 1)
        self.assertEqual(client._requests_processed, 2)
        #
        result = yield multi_async((client(b'ciao'),
                                    client(b'pippo'),
                                    client(b'foo')))
        self.assertEqual(len(result), 3)
        self.assertTrue(b'ciao' in result)
        self.assertTrue(b'pippo' in result)
        self.assertTrue(b'foo' in result)
        self.assertEqual(client.pool.in_use, 0)
        self.assertEqual(client.pool.available, 2)
        self.assertEqual(client.sessions, 2)
        self.assertEqual(client._requests_processed, 5)
        #
        # drop a connection
        conn1 = client.pool._queue.get_nowait()
        conn1.close()
        conn2 = client.pool._queue.get_nowait()
        client.pool._queue.put_nowait(conn1)
        client.pool._queue.put_nowait(conn2)
        #
        result = yield multi_async((client(b'ciao'),
                                    client(b'pippo'),
                                    client(b'foo')))
        self.assertEqual(len(result), 3)
        self.assertEqual(client.pool.in_use, 0)
        self.assertEqual(client.pool.available, 2)
        self.assertEqual(client.sessions, 3)
        self.assertEqual(client._requests_processed, 8)
        #
        client.pool.close()
        self.assertEqual(client.pool.in_use, 0)
        self.assertEqual(client.pool.available, 0)
        self.assertEqual(client.sessions, 3)
        self.assertEqual(client._requests_processed, 8)



@dont_run_with_thread
class TestEchoServerProcess(TestEchoServerThread):
    concurrency = 'process'
