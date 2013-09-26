'''Redis pool.'''
from pulsar.apps.test import unittest
from pulsar.apps.redis import RedisPool

available = bool(RedisPool.consumer_factory)

if available:
    from pulsar.apps.redis.client import HAS_C_EXTENSIONS, RedisParser
else:
    HAS_C_EXTENSIONS = False
    RedisParser = None


@unittest.skipUnless(available, 'Requires redis-py installed')
class RedisTest(unittest.TestCase):

    @classmethod
    def parser_class(cls):
        return None

    @classmethod
    def setUpClass(cls):
        cls.pool = RedisPool(timeout=30, parser=cls.parser_class())

    @classmethod
    def parser(cls):
        return cls.pool.parser()

    def client(self, **kw):
        backend = self.cfg.backend_server or 'redis://127.0.0.1:6379'
        return self.pool.from_connection_string(backend, **kw)


class PythonParser(object):

    @classmethod
    def parser_class(cls):
        return RedisParser

    def test_redis_parser(self):
        parser = self.parser()
        self.assertTrue(hasattr(parser, '_current'))
        self.assertTrue(hasattr(parser, '_inbuffer'))


class TestRedisPool(RedisTest):

    def test_pipeline(self):
        client = self.client()
        pipe = client.pipeline()
        pipe.ping()
        pipe.echo('Hello')
        result = yield pipe.execute()
        self.assertTrue(result)

    def test_pool(self):
        pool = self.pool
        self.assertEqual(pool.encoding, 'utf-8')
        self.assertEqual(pool.timeout, 30)

    def test_client(self):
        client = self.client(db=9, password='foo')
        self.assertEqual(client.connection_pool, self.pool)
        self.assertTrue(client.connection_info)
        self.assertIsInstance(client.connection_info.address, tuple)
        self.assertEqual(client.connection_info.db, 9)
        self.assertEqual(client.connection_info.password, 'foo')

    def test_ping(self):
        client = self.client()
        result = yield client.ping()
        self.assertEqual(result, True)
        result = yield client.echo('Hello!')
        self.assertEqual(result, b'Hello!')

    def test_echo_new_connection(self):
        client = self.client()
        db = 11
        if client.connection_info.db == db:
            db = 12
        client = self.client(db=db, full_response=True)
        password = int(bool(client.connection_info.password))
        response = yield client.echo('Hello!').on_finished
        self.assertEqual(response.result, b'Hello!')
        connection = response.connection
        self.assertEqual(connection.processed, 2+password)
        response = yield client.echo('Ciao!').on_finished
        self.assertEqual(response.result, b'Ciao!')
        self.assertEqual(connection, response.connection)
        self.assertEqual(connection.processed, 3+password)


@unittest.skipUnless(HAS_C_EXTENSIONS , 'Requires cython extensions')
class TestRedisPoolPythonParser(PythonParser, TestRedisPool):
    pass
