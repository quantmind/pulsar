from pulsar import new_event_loop, HAS_C_EXTENSIONS, get_actor

from .pulsards import unittest, RedisCommands, Scripting, create_store


addr = get_actor().cfg.get('redis_server')
sync_store = create_store(addr, loop=new_event_loop())
try:
    sync_store.client().ping()
except Exception:
    sync_store = None


@unittest.skipUnless(sync_store, 'Requires a running redis server')
class TestRedisStore(RedisCommands, Scripting, unittest.TestCase):
    store = None

    @classmethod
    def setUpClass(cls):
        addr = 'redis://%s' % cls.cfg.redis_server
        cls.store = cls.create_store(addr)
        cls.sync_store = sync_store
        cls.client = cls.store.client()


@unittest.skipUnless(HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestRedisStorePyParser(TestRedisStore):
    pass
