from pulsar import new_event_loop, HAS_C_EXTENSIONS
from pulsar.apps.test import check_redis

from .pulsards import unittest, RedisCommands, Scripting, create_store


@unittest.skipUnless(check_redis(), 'Requires a running redis server')
class TestRedisStore(RedisCommands, Scripting, unittest.TestCase):
    store = None

    @classmethod
    def setUpClass(cls):
        addr = 'redis://%s' % cls.cfg.redis_server
        cls.store = cls.create_store(addr)
        cls.sync_store = create_store(addr, loop=new_event_loop())
        cls.client = cls.store.client()


@unittest.skipUnless(HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestRedisStorePyParser(TestRedisStore):
    pass
