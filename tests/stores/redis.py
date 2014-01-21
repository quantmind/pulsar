from pulsar import new_event_loop, HAS_C_EXTENSIONS

from .pulsards import unittest, RedisCommands, Scripting, create_store


class TestRedisStore(RedisCommands, Scripting, unittest.TestCase):
    store = None

    @classmethod
    def setUpClass(cls):
        addr = 'redis://%s' % cls.cfg.redis_server
        cls.store = cls.create_store(addr)
        cls.sync_store = cls.create_store(addr, loop=new_event_loop())
        cls.client = cls.store.client()


@unittest.skipUnless(HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestRedisStorePythonParser(TestRedisStore):
    pass
