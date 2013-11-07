from pulsar import send
from pulsar.apps.test import unittest
from pulsar.apps.data import KeyValueStore, create_store


class TestKeyValueStoreBase(unittest.TestCase):
    concurrency = 'thread'
    app = None

    @classmethod
    def setUpClass(cls):
        server = KeyValueStore(name=cls.__name__.lower(),
                               bind='127.0.0.1:0', workers=1,
                               concurrency=cls.concurrency)
        cls.app = yield send('arbiter', 'run', server)
        cls.store = create_store('pulsar://%s:%s' % cls.app.address)
        cls.sync_store = create_store('pulsar://%s:%s' % cls.app.address,
                                      force_sync=True)
        cls.client = cls.store.client()

    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)


class TestKeyValueStore(TestKeyValueStoreBase):

    def test_ping(self):
        result = yield self.client.ping()
        self.assertTrue(result)

    def test_set(self):
        result = yield self.client.set('bla', 'foo')
        self.assertTrue(result)

    def test_get(self):
        yield self.client.set('blax', 'foo')
        result = yield self.client.get('blax')
        self.assertEqual(result, b'foo')
        result = yield self.client.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        self.assertEqual(result, None)

    def test_mget(self):
        yield self.client.set('blaxx', 'foox')
        yield self.client.set('blaxxx', 'fooxx')
        result = yield self.client.mget('blaxx', 'blaxxx', 'xxxxxxxxxxxxxx')
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], b'foox')
        self.assertEqual(result[1], b'fooxx')
        self.assertEqual(result[2], None)

    def test_dbsize(self):
        yield self.client.set('one_at_least', 'foo')
        result = yield self.client.dbsize()
        self.assertTrue(result >= 1)

    def test_info(self):
        info = yield self.client.info()
        self.assertIsInstance(info, dict)

    def test_time(self):
        t = yield self.client.time()
        self.assertIsInstance(t, tuple)
        total = t[0] + 0.000001*t[1]

    def test_sync(self):
        client = self.sync_store.client()
        self.assertEqual(client.echo('Hello'), b'Hello')
