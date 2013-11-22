from pulsar import send
from pulsar.utils.security import random_string
from pulsar.apps.test import unittest
from pulsar.apps.data import KeyValueStore, create_store


class RedisCommands(object):

    def randomkey(self):
        return random_string()

    def test_eval(self):
        result = yield self.client.eval('return "Hello"')
        self.assertEqual(result, b'Hello')
        result = yield self.client.eval("return {ok='OK'}")
        self.assertEqual(result, b'OK')

    def test_eval_with_keys(self):
        result = yield self.client.eval("return {KEYS, ARGV}",
                                        ('a', 'b'),
                                        ('first', 'second', 'third'))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], [b'a', b'b'])
        self.assertEqual(result[1], [b'first', b'second', b'third'])

class d:
    ###########################################################################
    ##    CONNECTION
    def test_ping(self):
        result = yield self.client.ping()
        self.assertTrue(result)

    def test_echo(self):
        result = yield self.client.echo('Hello')
        self.assertEqual(result, b'Hello')

    ###########################################################################
    ##    SERVER
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

    ###########################################################################
    ##    KEYS
    def test_append(self):
        c = self.client
        key = self.randomkey()
        yield self.async.assertEqual(c.append(key, 'a1'), 2)
        yield self.async.assertEqual(c[key], b'a1')
        yield self.async.assertEqual(c.append(key, 'a2'), 4)
        yield self.async.assertEqual(c[key], b'a1a2')

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

    ###########################################################################
    ##    PUBSUB
    def test_handler(self):
        client = self.client
        pubsub = client.pubsub()
        self.assertEqual(client.store, pubsub.store)
        self.assertEqual(client.store._loop, pubsub._loop)
        self.assertEqual(pubsub._connection, None)

    def test_subscribe_one(self):
        pubsub1 = self.client.pubsub()
        self.assertFalse(pubsub1._connection)
        # Subscribe to one channel
        yield pubsub1.subscribe('blaaaaaa')
        count = yield pubsub1.count('blaaaaaa')
        self.assertEqual(count, 1)
        #
        pubsub2 = self.client.pubsub()
        yield pubsub2.subscribe('blaaaaaa')
        count = yield pubsub1.count('blaaaaaa')
        self.assertEqual(count, 2)

    def test_subscribe_many(self):
        pubsub = self.client.pubsub()
        yield pubsub.subscribe('foooo1', 'foooo2', 'foooo3')
        channels = yield pubsub.channels('fooo*')
        self.assertEqual(len(channels), 3)
        count = yield pubsub.count('foooo1')
        self.assertEqual(count, 1)
        count = yield pubsub.count('foooo2')
        self.assertEqual(count, 1)
        count = yield pubsub.count('foooo3')
        self.assertEqual(count, 1)

    def test_publish(self):
        pubsub = self.client.pubsub()
        self.called = False

        def check_message(message):
            self.assertEqual(message[0], b'chat')
            self.assertEqual(message[1], b'Hello')
            self.called = True

        pubsub.bind_event('on_message', check_message)
        yield pubsub.subscribe('chat')
        result = yield pubsub.publish('chat', 'Hello')
        self.assertTrue(result>=0)
        self.assertTrue(self.called)

    ###########################################################################
    ##    SYNCHRONOUS CLIENT
    def test_sync(self):
        client = self.sync_store.client()
        self.assertEqual(client.echo('Hello'), b'Hello')


class TestPulsarStore(RedisCommands, unittest.TestCase):
    concurrency = 'thread'
    app = None

    @classmethod
    def setUpClass(cls):
        server = KeyValueStore(name=cls.__name__.lower(),
                               bind='127.0.0.1:0',
                               concurrency=cls.concurrency)
        cls.app = yield send('arbiter', 'run', server)
        cls.store = create_store('pulsar://%s:%s/9' % cls.app.address)
        cls.sync_store = create_store('pulsar://%s:%s/10' % cls.app.address,
                                      force_sync=True)
        cls.client = cls.store.client()

    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)
