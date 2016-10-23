from pulsar import HAS_C_EXTENSIONS
from pulsar.apps.test import check_server
from pulsar.apps.data import RedisScript

from tests.stores.test_pulsards import unittest, RedisCommands, create_store
from tests.stores.lock import RedisLockTests
from tests.stores.channels import ChannelsTests


OK = check_server('redis')


@unittest.skipUnless(OK, 'Requires a running Redis server')
class TestRedisStore(RedisCommands,
                     RedisLockTests,
                     ChannelsTests,
                     unittest.TestCase):
    store = None

    @classmethod
    def namespace(cls):
        return cls.__name__.lower()

    @classmethod
    def setUpClass(cls):
        addr = cls.cfg.redis_server
        if not addr.startswith('redis://'):
            addr = 'redis://%s' % cls.cfg.redis_server
        cls.store = create_store(addr, pool_size=3, namespace=cls.namespace())
        cls.client = cls.store.client()

    async def test_script(self):
        script = RedisScript("return 1")
        self.assertFalse(script.sha)
        self.assertTrue(script.script)
        result = await script(self.client)
        self.assertEqual(result, 1)
        self.assertTrue(script.sha)
        self.assertTrue(script.sha in self.client.store.loaded_scripts)
        result = await script(self.client)
        self.assertEqual(result, 1)

    async def test_eval(self):
        result = await self.client.eval('return "Hello"')
        self.assertEqual(result, b'Hello')
        result = await self.client.eval("return {ok='OK'}")
        self.assertEqual(result, b'OK')

    async def test_eval_with_keys(self):
        result = await self.client.eval("return {KEYS, ARGV}",
                                        ('a', 'b'),
                                        ('first', 'second', 'third'))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [b'a', b'b'])
        self.assertEqual(result[1], [b'first', b'second', b'third'])


@unittest.skipUnless(OK and HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestRedisStorePyParser(TestRedisStore):
    pass
