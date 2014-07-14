import unittest

try:
    from pulsar.apps import greenio
    run_in_greenlet = greenio.run_in_greenlet
except ImportError:
    greenio = None
    run_in_greenlet = lambda x: x

from pulsar import Future, send, multi_async, get_event_loop

from examples.echo.manage import server, Echo


def raise_error():
    raise RuntimeError


class EchoGreen(Echo):
    '''An echo client which uses greenlets to provide implicit
    asynchronous code'''
    @run_in_greenlet
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
        cls.server_cfg = yield send('arbiter', 'run', s)
        cls.client = EchoGreen(cls.server_cfg.addresses[0])

    @classmethod
    def tearDownClass(cls):
        if cls.server_cfg:
            return send('arbiter', 'kill_actor', cls.server_cfg.name)

    def test_local(self):
        data = greenio.local()
        data.bla = 56
        self.assertEqual(data.bla, 56)

        def _test_bla():
            self.assertFalse(hasattr(data, 'bla'))
            data.bla = 89
            self.assertEqual(data.bla, 89)
        gr = greenio.PulsarGreenlet(_test_bla)
        gr.switch()
        self.assertEqual(data.bla, 56)

    def test_pool(self):
        pool = greenio.GreenPool()
        self.assertTrue(pool._loop)
        self.assertEqual(pool._loop, get_event_loop())
        self.assertFalse(pool._greenlets)
        future = pool.submit(lambda: 'Hi!')
        self.assertIsInstance(future, Future)
        result = yield future
        self.assertEqual(result, 'Hi!')
        self.assertEqual(len(pool._greenlets), 1)
        self.assertEqual(len(pool._available), 1)

    def test_error_in_pool(self):
        # Test an error
        pool = greenio.GreenPool()
        yield self.async.assertRaises(RuntimeError, pool.submit, raise_error)
        self.assertEqual(len(pool._greenlets), 1)
        self.assertEqual(len(pool._available), 1)

    def test_echo(self):
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
