import binascii

import pulsar
from pulsar import new_event_loop
from pulsar.utils.security import random_string
from pulsar.utils.structures import Zset
from pulsar.apps.test import unittest
from pulsar.apps.data import (PulsarDS, create_store, redis_parser,
                              ResponseError)


class RedisCommands(object):
    redis_py_parser = False

    @classmethod
    def create_store(cls, address, **kw):
        if cls.redis_py_parser:
            kw['parser_class'] = redis_parser(True)
        return create_store(address, **kw)

    def randomkey(self):
        return random_string()

    def test_watch(self):
        key1 = self.randomkey()
        key2 = key1 + '2'
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.watch(key1, key2), True)
        yield eq(c.unwatch(), True)

    ###########################################################################
    ##    BAD REQUESTS
    #def test_no_command(self):
    #    yield self.async.assertRaises(ResponseError, self.store.execute)

    #def test_bad_command(self):
    #    yield self.async.assertRaises(ResponseError, self.store.execute, 'foo')


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
    ### SET COMMANDS ###
    def test_sadd_scard(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        members = (b'1', b'2', b'3', b'2')
        yield eq(c.sadd(key, *members), 3)
        yield eq(c.smembers(key), set(members))
        yield eq(c.scard(key), 3)

    def test_sdiff(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sdiff(key, key2), set((b'1', b'2', b'3')))
        yield eq(c.sadd(key2, 2, 3), 2)
        yield eq(c.sdiff(key, key2), set([b'1']))

    def test_sdiffstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sdiffstore(des, key, key2), 3)
        yield eq(c.smembers(des), set([b'1', b'2', b'3']))
        yield eq(c.sadd(key2, 2, 3), 2)
        yield eq(c.sdiffstore(des, key, key2), 1)
        yield eq(c.smembers(des), set([b'1']))

    def test_sinter(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sinter(key, key2), set())
        yield eq(c.sadd(key2, 2, 3), 2)
        yield eq(c.sinter(key, key2), set([b'2', b'3']))

    def test_sinterstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sinterstore(des, key, key2), 0)
        yield eq(c.smembers(des), set())
        yield eq(c.sadd(key2, 2, 3), 2)
        yield eq(c.sinterstore(des, key, key2), 2)
        yield eq(c.smembers(des), set([b'2', b'3']))

    def test_sismember(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sismember(key, 1), True)
        yield eq(c.sismember(key, 2), True)
        yield eq(c.sismember(key, 3), True)
        yield eq(c.sismember(key, 4), False)

    def test_smove(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.smove(key, key2, 1), False)
        yield eq(c.sadd(key, 1, 2), 2)
        yield eq(c.sadd(key2, 3, 4), 2)
        yield eq(c.smove(key, key2, 1), True)
        yield eq(c.smembers(key), set([b'2']))
        yield eq(c.smembers(key2), set([b'1', b'3', b'4']))

    def test_spop(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        value = yield c.spop(key)
        self.assertTrue(value in set([b'1', b'2', b'3']))
        yield eq(c.smembers(key), set([b'1', b'2', b'3']) - set([value]))

    def test_srandmember(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        value = yield c.srandmember(key)
        self.assertTrue(value in set((b'1', b'2', b'3')))
        yield eq(c.smembers(key), set((b'1', b'2', b'3')))

    def test_srandmember_multi_value(self):
        s = [b'1', b'2', b'3']
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, *s), 3)
        randoms = yield c.srandmember(key, 2)
        self.assertEqual(len(randoms), 2)
        self.assertEqual(set(randoms).intersection(s), set(randoms))

    def test_srem(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3, 4), 4)
        yield eq(c.srem(key, 5), 0)
        yield eq(c.srem(key, 5), 0)
        yield eq(c.srem(key, 2, 4), 2)
        yield eq(c.smembers(key), set([b'1', b'3']))

    def test_sunion(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sunion(key, key2), set((b'1', b'2', b'3')))
        yield eq(c.sadd(key2, 2, 3, 4), 3)
        yield eq(c.sunion(key, key2), set((b'1', b'2', b'3', b'4')))

    def test_sunionstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.sadd(key, 1, 2, 3), 3)
        yield eq(c.sunionstore(des, key, key2), 3)
        yield eq(c.smembers(des), set([b'1', b'2', b'3']))
        yield eq(c.sadd(key2, 2, 3, 4), 3)
        yield eq(c.sunionstore(des, key, key2), 4)
        yield eq(c.smembers(des), set([b'1', b'2', b'3', b'4']))
    ###########################################################################
    ##    SORTED SETS
    def test_zadd_zcard(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        members = (b'1', b'2', b'3', b'2')
        yield eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield eq(c.zrange(key, 0, -1), [b'a1', b'a2', b'a3'])
        yield eq(c.zcard(key), 3)

    def test_zcount(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield eq(c.zcount(key, '-inf', '+inf'), 3)
        yield eq(c.zcount(key, '(1', 2), 1)
        yield eq(c.zcount(key, '(1', '(3'), 1)
        yield eq(c.zcount(key, 1, 3), 3)

    def test_zincrby(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield eq(c.zincrby(key, 1, 'a2'), 3.0)
        yield eq(c.zincrby(key, 5, 'a3'), 8.0)
        yield eq(c.zscore(key, 'a2'), 3.0)
        yield eq(c.zscore(key, 'a3'), 8.0)
        yield eq(c.zscore(key, 'blaaa'), None)

    def test_zinterstore_sum(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield eq(c.zinterstore(des, (key1, key2, key3)), 2)
        yield eq(c.zrange(des, 0, -1, withscores=True),
                 Zset(((8.0, b'a3'), (9.0, b'a1'))))

    def test_zinterstore_max(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield eq(c.zinterstore(des, (key1, key2, key3), aggregate='max'), 2)
        yield eq(c.zrange(des, 0, -1, withscores=True),
                 Zset(((5.0, b'a3'), (6.0, b'a1'))))

    def test_zinterstore_min(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield eq(c.zinterstore(des, (key1, key2, key3), aggregate='min'), 2)
        yield eq(c.zrange(des, 0, -1, withscores=True),
                 Zset(((1.0, b'a3'), (1.0, b'a1'))))

    def test_zinterstore_with_weights(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield eq(c.zinterstore(des, (key1, key2, key3), weights=(1, 2, 3)), 2)
        yield eq(c.zrange(des, 0, -1, withscores=True),
                 Zset(((20.0, b'a3'), (23.0, b'a1'))))

    def test_zrange(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield eq(c.zrange(key, 0, 1), [b'a1', b'a2'])
        yield eq(c.zrange(key, 1, 2), [b'a2', b'a3'])
        yield eq(c.zrange(key, 0, 1, withscores=True),
                 Zset([(1, b'a1'), (2, b'a2')]))
        yield eq(c.zrange(key, 1, 2, withscores=True),
                 Zset([(2, b'a2'), (3, b'a3')]))

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
    ##    TRANSACTION
    def test_watch(self):
        key1 = self.randomkey()
        key2 = key1 + '2'
        result = yield self.client.watch(key1)
        self.assertEqual(result, 1)


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
        self.assertFalse(client.store._loop.is_running())
        self.assertEqual(client.echo('Hello'), b'Hello')


class TestPulsarStore(RedisCommands, unittest.TestCase):
    concurrency = 'thread'
    app = None

    @classmethod
    def setUpClass(cls):
        server = PulsarDS(name=cls.__name__.lower(),
                          bind='127.0.0.1:0',
                          concurrency=cls.concurrency,
                          redis_py_parser=cls.redis_py_parser)
        cls.app = yield pulsar.send('arbiter', 'run', server)
        cls.store = cls.create_store('pulsar://%s:%s/9' % cls.app.address)
        cls.sync_store = cls.create_store(
            'pulsar://%s:%s/10' % cls.app.address, loop=new_event_loop())
        cls.client = cls.store.client()

    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield pulsar.send('arbiter', 'kill_actor', cls.app.name)


@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS , 'Requires cython extensions')
class TestPulsarStorePyParser(TestPulsarStore):
    redis_py_parser = True
