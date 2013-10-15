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
        cls.pool = Echo()
        cls.echo = cls.pool.client(cls.server.address)

    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield send('arbiter', 'kill_actor', cls.server.name)

    def test_server(self):
        self.assertTrue(self.server)
        self.assertEqual(self.server.callable, EchoServerProtocol)
        self.assertTrue(self.server.address)

    def test_ping(self):
        result = yield self.echo(b'ciao luca')
        self.assertEqual(result, b'ciao luca')

    def test_large(self):
        '''Echo a 3MB message'''
        msg = b''.join((b'a' for x in range(2**13)))
        result = yield self.echo(msg)
        self.assertEqual(result, msg)

    def testTimeIt(self):
        msg = b''.join((b'a' for x in range(2**10)))
        response = self.pool.timeit(10, self.server.address, msg)
        yield response
        self.assertTrue(response.locked_time >= 0)
        self.assertTrue(response.total_time >= response.locked_time)
        self.assertEqual(response.num_failures, 0)

    def test_multi(self):
        result = yield multi_async((self.echo(b'ciao'),
                                    self.echo(b'pippo'),
                                    self.echo(b'foo')))
        self.assertEqual(len(result), 3)
        self.assertTrue(b'ciao' in result)
        self.assertTrue(b'pippo' in result)
        self.assertTrue(b'foo' in result)

    # TESTS FOR PROTOCOLS AND CONNECTIONS
    def test_client(self):
        c = self.pool
        yield self.test_multi()
        self.assertTrue(len(c.connection_pools), 1)
        self.assertTrue(c.available_connections)

    def test_info(self):
        info = yield send(self.server.name, 'info')
        self.assertIsInstance(info, dict)
        self.assertEqual(info['actor']['name'], self.server.name)
        self.assertEqual(info['actor']['concurrency'], self.concurrency)

    def test_connection(self):
        pool = Echo(full_response=True)
        echo = pool.client(self.server.address)
        response = echo(b'test connection')
        self.assertTrue(str(response.connection))
        yield response.on_finished
        self.assertEqual(response.buffer, b'test connection')
        connection = response.connection
        self.assertTrue(str(connection))
        self.assertEqual(str(connection.transport)[:4], 'TCP ')


@dont_run_with_thread
class TestEchoServerProcess(TestEchoServerThread):
    concurrency = 'process'
