import unittest
import asyncio

from pulsar import Future, send, multi_async, get_event_loop

from examples.echo.manage import server, Echo

try:
    from pulsar.apps import greenio
    run_in_greenlet = greenio.run_in_greenlet
except ImportError:
    greenio = None

    def run_in_greenlet(x):
        return x


def raise_error():
    raise RuntimeError


class EchoGreen(Echo):
    '''An echo client which uses greenlets to provide implicit
    asynchronous code'''

    def __call__(self, message):
        connection = greenio.wait(self.pool.connect())
        with connection:
            consumer = connection.current_consumer()
            consumer.start(message)
            result = greenio.wait(consumer.on_finished)
            return consumer if self.full_response else consumer.buffer


@unittest.skipUnless(greenio, 'Requires the greenlet package')
class TestGreenIO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0')
        cls.server_cfg = yield from send('arbiter', 'run', s)
        cls.client = EchoGreen(cls.server_cfg.addresses[0])

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    def test_pool(self):
        pool = greenio.GreenPool()
        self.assertTrue(pool._loop)
        self.assertEqual(pool._loop, get_event_loop())
        self.assertFalse(pool._greenlets)
        future = pool.submit(lambda: 'Hi!')
        self.assertIsInstance(future, Future)
        result = yield from future
        self.assertEqual(result, 'Hi!')
        self.assertEqual(len(pool._greenlets), 1)
        self.assertEqual(len(pool._available), 1)

    def test_error_in_pool(self):
        # Test an error
        pool = greenio.GreenPool()
        yield from self.async.assertRaises(RuntimeError, pool.submit,
                                           raise_error)
        self.assertEqual(len(pool._greenlets), 1)
        self.assertEqual(len(pool._available), 1)

    @run_in_greenlet
    def test_echo(self):
        result = self.client(b'ciao luca')
        self.assertEqual(result, b'ciao luca')

    @run_in_greenlet
    def test_large(self):
        '''Echo a 3MB message'''
        msg = b''.join((b'a' for x in range(2**13)))
        result = self.client(msg)
        self.assertEqual(result, msg)

    def test_shutdown(self):
        # Test an error
        pool = greenio.GreenPool()
        yield from self.async.assertEqual(pool.submit(lambda: 'OK'), 'OK')
        self.assertEqual(len(pool._greenlets), 1)
        self.assertEqual(len(pool._available), 1)
        a = pool.submit(lambda: 'a')
        b = pool.submit(lambda: 'b')
        self.assertEqual(len(pool._greenlets), 2)
        self.assertEqual(len(pool._available), 0)
        result = yield from multi_async([a, b])
        self.assertEqual(result[0], 'a')
        self.assertEqual(result[1], 'b')
        self.assertEqual(len(pool._greenlets), 2)
        self.assertEqual(len(pool._available), 2)
        yield from pool.shutdown()
        self.assertEqual(len(pool._greenlets), 0)
        self.assertEqual(len(pool._available), 0)
