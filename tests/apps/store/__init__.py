from pulsar import send
from pulsar.apps.test import unittest
from pulsar.apps.data import KeyValueStore, create_store


class TestKeyValueStore(unittest.TestCase):
    concurrency = 'thread'
    app = None

    @classmethod
    def setUpClass(cls):
        server = KeyValueStore(name=cls.__name__.lower(),
                               bind='127.0.0.1:0', workers=1,
                               concurrency=cls.concurrency)
        cls.app = yield send('arbiter', 'run', server)
        cls.store = create_store('pulsar://%s:%s' % cls.app.address)
        cls.client = cls.store.client()

    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)

    def test_ping(self):
        result = yield self.client.ping()
        self.assertTrue(result)
