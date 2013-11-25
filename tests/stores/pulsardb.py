import binascii

import pulsar
from pulsar.utils.security import random_string
from pulsar.apps.test import unittest
from pulsar.apps.data import KeyValueStore, create_store, redis_parser


class RedisCommands(object):
    redis_py_parser = False

    @classmethod
    def create_store(cls, address, **kw):
        if cls.redis_py_parser:
            kw['parser_class'] = redis_parser(True)
        return create_store(address, **kw)

    def randomkey(self):
        return random_string()

    ###########################################################################
    ##    STRINGS
    def test_append(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.append(key, 'a1'), 2)
        yield eq(c.get(key), b'a1')
        yield eq(c.append(key, 'a2'), 4)
        yield eq(c.get(key), b'a1a2')

    def test_bitcount(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.setbit(key, 5, 1), 0)
        yield eq(c.bitcount(key), 1)
        yield eq(c.setbit(key, 6, 1), 0)
        yield eq(c.bitcount(key), 2)
        yield eq(c.setbit(key, 5, 0), 1)
        yield eq(c.bitcount(key), 1)
        yield eq(c.setbit(key, 9, 1), 0)
        yield eq(c.setbit(key, 17, 1), 0)
        yield eq(c.setbit(key, 25, 1), 0)
        yield eq(c.setbit(key, 33, 1), 0)
        yield eq(c.bitcount(key), 5)
        yield eq(c.bitcount(key, 0, -1), 5)
        yield eq(c.bitcount(key, 2, 3), 2)
        yield eq(c.bitcount(key, 2, -1), 3)
        yield eq(c.bitcount(key, -2, -1), 2)
        yield eq(c.bitcount(key, 1, 1), 1)

    def test_bitop_not_empty_string(self):
        key = self.randomkey()
        des =  key + 'd'
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.set(key, ''), True)
        yield eq(c.bitop('not', des, key), 0)
        yield eq(c.get(des), None)

    def test_bitop_not(self):
        key = self.randomkey()
        des = key + 'd'
        c = self.client
        eq = self.async.assertEqual
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        yield eq(c.set(key, test_str), True)
        yield eq(c.bitop('not', des, key), 4)
        result = yield c.get(des)
        self.assertEqual(int(binascii.hexlify(result), 16), correct)

    def test_bitop_not_in_place(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        yield eq(c.set(key, test_str), True)
        yield eq(c.bitop('not', key, key), 4)
        result = yield c.get(key)
        assert int(binascii.hexlify(result), 16) == correct

    def test_bitop_single_string(self):
        key = self.randomkey()
        des = key + 'd'
        c = self.client
        eq = self.async.assertEqual
        test_str = b'\x01\x02\xFF'
        yield eq(c.set(key, test_str), True)
        yield eq(c.bitop('and', key+'1', key), 3)
        yield eq(c.bitop('or', key+'2', key), 3)
        yield eq(c.bitop('xor', key+'3', key), 3)
        yield eq(c.get(key + '1'), test_str)
        yield eq(c.get(key + '2'), test_str)
        yield eq(c.get(key + '3'), test_str)

    def test_bitop_string_operands(self):
        c = self.client
        eq = self.async.assertEqual
        key1 = self.randomkey()
        key2 = key1 + '2'
        des1 = key1 + 'd1'
        des2 = key1 + 'd2'
        des3 = key1 + 'd3'
        yield eq(c.set(key1, b'\x01\x02\xFF\xFF'), True)
        yield eq(c.set(key2, b'\x01\x02\xFF'), True)
        yield eq(c.bitop('and', des1, key1, key2), 4)
        yield eq(c.bitop('or', des2, key1, key2), 4)
        yield eq(c.bitop('xor', des3, key1, key2), 4)
        res1 = yield c.get(des1)
        res2 = yield c.get(des2)
        res3 = yield c.get(des3)
        self.assertEqual(int(binascii.hexlify(res1), 16), 0x0102FF00)
        self.assertEqual(int(binascii.hexlify(res2), 16), 0x0102FFFF)
        self.assertEqual(int(binascii.hexlify(res3), 16), 0x000000FF)

    def test_decr(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.decr(key), -1)
        yield eq(c.get(key), b'-1')
        yield eq(c.decr(key), -2)
        yield eq(c.get(key), b'-2')
        yield eq(c.decr(key, 5), -7)
        yield eq(c.get(key), b'-7')

    def test_incr(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.incr(key), 1)
        yield eq(c.get(key), b'1')
        yield eq(c.incr(key), 2)
        yield eq(c.get(key), b'2')
        yield eq(c.incr(key, 5), 7)
        yield eq(c.get(key), b'7')

    def test_get(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield c.set(key, 'foo')
        yield eq(c.get(key), b'foo')
        yield eq(c.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'), None)

    def test_mget(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        key3 = key2 + 'y'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.set(key1, 'foox'), True)
        yield eq(c.set(key2, 'fooxx'), True)
        yield eq(c.mget(key1, key2, key3), [b'foox', b'fooxx', None])

    ###########################################################################
    ##    HASHES
    def test_hdel(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}), True)
        yield eq(c.hdel(key, 'f1', 'f2', 'gh'), 2)
        yield eq(c.hdel(key, 'fgf'), 0)
        yield eq(c.type(key), 'hash')
        yield eq(c.hdel(key, 'f3'), 1)
        yield eq(c.type(key), 'none')

    ###########################################################################
    ##    LISTS
    def test_blpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.rpush(key1, 1, 2), 2)
        yield eq(c.rpush(key2, 3, 4), 2)
        yield eq(c.blpop((key2, key1), 1), (bk2, b'3'))
        yield eq(c.blpop((key2, key1), 1), (bk2, b'4'))
        yield eq(c.blpop((key2, key1), 1), (bk1, b'1'))
        yield eq(c.blpop((key2, key1), 1), (bk1, b'2'))
        yield eq(c.blpop((key2, key1), 1), None)
        yield eq(c.rpush(key1, '1'), 1)
        yield eq(c.blpop(key1, 1), (bk1, b'1'))

    ###########################################################################
    ##    SETS

    ###########################################################################
    ##    ORDERED SETS

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
    ##    PUBSUB
    def test_handler(self):
        client = self.client
        pubsub = client.pubsub()
        self.assertEqual(client.store, pubsub.store)
        self.assertEqual(client.store._loop, pubsub._loop)
        self.assertEqual(pubsub._connection, None)

    def test_subscribe_one(self):
        key = self.randomkey()
        pubsub1 = self.client.pubsub()
        self.assertFalse(pubsub1._connection)
        # Subscribe to one channel
        yield pubsub1.subscribe(key)
        count = yield pubsub1.count(key)
        self.assertEqual(len(count), 1)
        self.assertEqual(count[key.encode('utf-8')], 1)
        #
        pubsub2 = self.client.pubsub()
        yield pubsub2.subscribe(key)
        count = yield pubsub1.count(key)
        self.assertEqual(len(count), 1)
        self.assertEqual(count[key.encode('utf-8')], 2)

    def test_subscribe_many(self):
        base = self.randomkey()
        key1 = base + '_a'
        key2 = base + '_b'
        key3 = base + '_c'
        key4 = base + 'x'
        pubsub = self.client.pubsub()
        yield pubsub.subscribe(key1, key2, key3, key4)
        channels = yield pubsub.channels(base + '_*')
        self.assertEqual(len(channels), 3)
        count = yield pubsub.count(key1, key2, key3)
        self.assertEqual(len(count), 3)
        self.assertEqual(count[key1.encode('utf-8')], 1)
        self.assertEqual(count[key2.encode('utf-8')], 1)
        self.assertEqual(count[key3.encode('utf-8')], 1)

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
    ##    SCRIPTING
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
                               concurrency=cls.concurrency,
                               redis_py_parser=cls.redis_py_parser)
        cls.app = yield pulsar.send('arbiter', 'run', server)
        cls.store = cls.create_store('pulsar://%s:%s/9' % cls.app.address)
        cls.sync_store = cls.create_store(
            'pulsar://%s:%s/10' % cls.app.address, force_sync=True)
        cls.client = cls.store.client()

    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield pulsar.send('arbiter', 'kill_actor', cls.app.name)


#@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS , 'Requires cython extensions')
#class TestPulsarStorePyParser(TestPulsarStore):
#    redis_py_parser = True
