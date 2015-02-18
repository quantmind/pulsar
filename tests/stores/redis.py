from pulsar import new_event_loop, HAS_C_EXTENSIONS
from pulsar.apps.test import check_server

from .pulsards import unittest, RedisCommands, create_store


OK = check_server('redis')


@unittest.skipUnless(OK, 'Requires a running Redis server')
class RedisDbTest(RedisCommands):
    pass


@unittest.skipUnless(OK, 'Requires a running redis server')
class TestRedisStore(RedisDbTest, unittest.TestCase):
    store = None

    @classmethod
    def setUpClass(cls):
        addr = cls.cfg.redis_server
        if not addr.startswith('redis://'):
            addr = 'redis://%s' % cls.cfg.redis_server
        namespace = cls.__name__.lower()
        cls.store = create_store(addr, pool_size=3, namespace=namespace)
        cls.client = cls.store.client()

    def test_eval(self):
        result = yield from self.client.eval('return "Hello"')
        self.assertEqual(result, b'Hello')
        result = yield from self.client.eval("return {ok='OK'}")
        self.assertEqual(result, b'OK')

    def test_eval_with_keys(self):
        result = yield from self.client.eval("return {KEYS, ARGV}",
                                             ('a', 'b'),
                                             ('first', 'second', 'third'))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [b'a', b'b'])
        self.assertEqual(result[1], [b'first', b'second', b'third'])


@unittest.skipUnless(OK and HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestRedisStorePyParser(TestRedisStore):
    pass
