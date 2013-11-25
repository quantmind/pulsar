import pulsar

from .pulsardb import unittest, RedisCommands, create_store


class TestRedisStore(RedisCommands, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.store = cls.create_store('redis://127.0.0.1:6379/9')
        cls.sync_store = cls.create_store('redis://127.0.0.1:6379/9',
                                          force_sync=True)
        cls.client = cls.store.client()


#@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS , 'Requires cython extensions')
#class TestRedisPoolPythonParser(TestRedisStore):
#    pass

