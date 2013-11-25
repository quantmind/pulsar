import pulsar

from .pulsardb import unittest, RedisCommands, create_store


class TestRedisStore(RedisCommands, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        addr = 'redis://%s' % cls.cfg.redis_server
        cls.store = cls.create_store(addr)
        cls.sync_store = cls.create_store(addr, force_sync=True)
        cls.client = cls.store.client()


#@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS , 'Requires cython extensions')
#class TestRedisPoolPythonParser(TestRedisStore):
#    pass

