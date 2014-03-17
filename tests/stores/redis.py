from pulsar import new_event_loop, HAS_C_EXTENSIONS
from pulsar.apps.test import check_server

from .pulsards import unittest, RedisCommands, Scripting, create_store


OK = check_server('redis')


@unittest.skipUnless(OK, 'Requires a running Redis server')
class RedisDbTest(object):

    @classmethod
    def create_store(cls, pool_size=2, namespace=None, **kw):
        addr = cls.cfg.redis_server
        if not addr.startswith('redis://'):
            addr = 'redis://%s' % cls.cfg.redis_server
        namespace = namespace or cls.name(cls.__name__)
        return create_store(addr, pool_size=pool_size, namespace=namespace,
                            **kw)


@unittest.skipUnless(check_server('redis'), 'Requires a running redis server')
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
