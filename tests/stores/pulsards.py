import binascii
import time
import unittest
from asyncio import Queue

import pulsar
from pulsar import new_event_loop
from pulsar.utils.security import random_string
from pulsar.utils.structures import Zset
from pulsar.apps.data import (PulsarDS, create_store, redis_parser,
                              ResponseError)


class Listener:

    def __init__(self):
        self._messages = Queue()

    def __call__(self, channel, message):
        self._messages.put_nowait((channel, message))

    def get(self):
        return self._messages.get()


class StringProtocol:

    def encode(self, message):
        return message

    def decode(self, message):
        return message.decode('utf-8')


class StoreMixin(object):
    client = None
    pulsar_app_cfg = None
    redis_py_parser = False

    @classmethod
    def create_store(cls, address, namespace=None, pool_size=2, **kw):
        if cls.redis_py_parser:
            kw['parser_class'] = redis_parser(True)
        if not namespace:
            namespace = cls.randomkey(6).lower()
        return create_store(address, namespace=namespace,
                            pool_size=pool_size, **kw)

    @classmethod
    def randomkey(cls, length=None):
        return random_string(length=length)

    @classmethod
    def create_pulsar_store(cls):
        server = PulsarDS(name=cls.__name__.lower(),
                          bind='127.0.0.1:0',
                          concurrency=cls.cfg.concurrency,
                          redis_py_parser=cls.redis_py_parser)
        cls.pulsar_app_cfg = yield pulsar.send('arbiter', 'run', server)
        cls.pulsards_uri = 'pulsar://%s:%s' % cls.pulsar_app_cfg.addresses[0]
        cls.store = cls.create_store('%s/9' % cls.pulsards_uri)

    @classmethod
    def tearDownClass(cls):
        if cls.pulsar_app_cfg is not None:
            yield pulsar.send('arbiter', 'kill_actor', cls.pulsar_app_cfg.name)

    def _remove_and_push(self, key, rem=1):
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.delete(key), rem)
        yield eq(c.rpush(key, 'bla'), 1)

    def _remove_and_sadd(self, key, rem=1):
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.delete(key), rem)
        yield eq(c.sadd(key, 'bla'), 1)


class RedisCommands(StoreMixin):

    def test_store(self):
        store = self.store
        self.assertEqual(len(store.namespace), 7)

    ###########################################################################
    ##    KEYS
    def test_dump_restore(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.dump(key), None)
        yield eq(c.set(key, 'hello'), True)
        value = yield c.dump(key)
        self.assertTrue(value)
        yield self.async.assertRaises(ResponseError, c.restore, key, 0, 'bla')
        yield eq(c.restore(key+'2', 0, value), True)
        yield eq(c.get(key+'2'), b'hello')

    def test_exists(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.exists(key), False)
        yield eq(c.set(key, 'hello'), True)
        yield eq(c.exists(key), True)
        yield eq(c.delete(key), 1)

    def test_expire_persist_ttl(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield self.async.assertRaises(ResponseError, c.expire, key, 'bla')
        yield eq(c.expire(key, 1), False)
        yield eq(c.set(key, 1), True)
        yield eq(c.expire(key, 10), True)
        ttl = yield c.ttl(key)
        self.assertTrue(ttl > 0 and ttl <= 10)
        yield eq(c.persist(key), True)
        yield eq(c.ttl(key), -1)
        yield eq(c.persist(key), False)

    def test_expireat(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield self.async.assertRaises(ResponseError, c.expireat, key, 'bla')
        t = int(time.time() + 3)
        yield eq(c.expireat(key, t), False)
        yield eq(c.set(key, 1), True)
        t = int(time.time() + 10)
        yield eq(c.expireat(key, t), True)
        ttl = yield c.ttl(key)
        self.assertTrue(ttl > 0 and ttl <= 10)
        yield eq(c.persist(key), True)
        yield eq(c.ttl(key), -1)
        yield eq(c.persist(key), False)

    def test_keys(self):
        key = self.randomkey()
        keya = '%s_a' % key
        keyb = '%s_a' % key
        keyc = '%sc' % key
        c = self.client
        eq = self.async.assertEqual
        keys_with_underscores = set([keya.encode('utf-8'),
                                     keyb.encode('utf-8')])
        keys = keys_with_underscores.union(set([keyc.encode('utf-8')]))
        yield eq(c.mset(keya, 1, keyb, 2, keyc, 3), True)
        k1 = yield c.keys('%s_*' % key)
        k2 = yield c.keys('%s*' % key)
        self.assertEqual(set(k1), keys_with_underscores)
        self.assertEqual(set(k2), keys)

    def test_move(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        db = 3 if c.store.database == 4 else 4
        yield eq(c.move(key, 'bla'), False)
        yield eq(c.move(key, db), False)
        yield eq(c.set(key, 'ciao'), True)
        yield eq(c.move(key, db), True)
        s2 = self.create_store(self.store.dns, database=db)
        c2 = s2.client()
        yield eq(c2.get(key), b'ciao')
        yield eq(c.exists(key), False)
        yield eq(c.set(key, 'foo'), True)
        yield eq(c.move(key, db), False)
        yield eq(c.exists(key), True)

    def test_randomkey(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.mset(key, 1, key+'a', 2, key+'b', 3), True)
        key = yield c.randomkey()
        yield eq(c.exists(key), True)

    def test_rename_renamenx(self):
        key = self.randomkey()
        des = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield self.async.assertRaises(ResponseError, c.rename, key, des)
        yield eq(c.set(key, 'hello'), True)
        yield self.async.assertRaises(ResponseError, c.rename, key, key)
        yield eq(c.rename(key, des), True)
        yield eq(c.exists(key), False)
        yield eq(c.get(des), b'hello')
        yield eq(c.set(key, 'ciao'), True)
        yield eq(c.renamenx(key, des), False)
        yield eq(c.renamenx(key, des+'a'), True)
        yield eq(c.exists(key), False)

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
    #    yield self.async.assertRaises(ResponseError, self.store.execute,
    #                                  'foo')
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
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.append, key, 'g')

    def test_bitcount(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.bitcount(key), 0)
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
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.bitcount, key)

    def test_bitop_not_empty_string(self):
        key = self.randomkey()
        des = key + 'd'
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

    def test_getbit(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.getbit(key, 5), 0)
        yield eq(c.setbit(key, 5, 1), 0)
        yield eq(c.getbit(key, 5), 1)
        yield self.async.assertRaises(ResponseError, c.getbit, key, -1)
        yield eq(c.getbit(key, 4), 0)
        yield eq(c.setbit(key, 4, 1), 0)
        # set bit 4
        yield eq(c.getbit(key, 4), 1)
        yield eq(c.getbit(key, 5), 1)
        # set bit 5 again
        yield eq(c.setbit(key, 5, 1), 1)
        yield eq(c.getbit(key, 5), 1)
        #
        yield eq(c.getbit(key, 30), 0)
        #
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.getbit, key, 1)

    def test_getrange(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.getrange(key, 0, 0), b'')
        yield eq(c.set(key, 'Hello there'), True)
        yield eq(c.getrange(key, 0, 0), b'H')
        yield eq(c.getrange(key, 0, 4), b'Hello')
        yield eq(c.getrange(key, 5, 5), b' ')
        yield eq(c.getrange(key, 20, 25), b'')
        yield eq(c.getrange(key, -5, -1), b'there')
        yield self.async.assertRaises(ResponseError, c.getrange, key, 1, 'b')
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.getrange, key, 1, 2)

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
        yield eq(c.hdel(key, 'f1', 'f2', 'gh'), 0)
        yield eq(c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}), True)
        yield eq(c.hdel(key, 'f1', 'f2', 'gh'), 2)
        yield eq(c.hdel(key, 'fgf'), 0)
        yield eq(c.type(key), 'hash')
        yield eq(c.hdel(key, 'f3'), 1)
        yield eq(c.type(key), 'none')
        yield self._remove_and_push(key, 0)
        yield self.async.assertRaises(ResponseError, c.hdel, key, 'foo')

    def test_hexists(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.hexists(key, 'foo'), False)
        yield eq(c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}), True)
        yield eq(c.hexists(key, 'f3'), True)
        yield eq(c.hexists(key, 'f5'), False)
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.hexists, key, 'foo')

    def test_hset_hget(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.hget(key, 'foo'), None)
        yield eq(c.hset(key, 'foo', 4), 1)
        yield eq(c.hget(key, 'foo'), b'4')
        yield eq(c.hset(key, 'foo', 6), 0)
        yield eq(c.hget(key, 'foo'), b'6')
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.hset, key, 'foo', 7)
        yield self.async.assertRaises(ResponseError, c.hget, key, 'foo')
        yield self.async.assertRaises(ResponseError, c.hmset, key, 'foo')

    def test_hgetall(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        h = {b'f1': b'1', b'f2': b'hello', b'f3': b'foo'}
        yield eq(c.hgetall(key), {})
        yield eq(c.hmset(key, h), True)
        yield eq(c.hgetall(key), h)
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.hgetall, key)

    def test_hincrby(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.hincrby(key, 'foo', 1), 1)
        yield eq(c.hincrby(key, 'foo', 2), 3)
        yield eq(c.hincrby(key, 'foo', -1), 2)
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.hincrby, key, 'foo', 3)

    def test_hincrbyfloat(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.hincrbyfloat(key, 'foo', 1), 1.0)
        yield eq(c.hincrbyfloat(key, 'foo', 2.5), 3.5)
        yield eq(c.hincrbyfloat(key, 'foo', -1.1), 2.4)
        yield self._remove_and_push(key)

    def test_hkeys_hvals_hlen_hmget(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        h = {b'f1': b'1', b'f2': b'hello', b'f3': b'foo'}
        yield eq(c.hkeys(key), [])
        yield eq(c.hvals(key), [])
        yield eq(c.hlen(key), 0)
        yield eq(c.hmset(key, h), True)
        keys = yield c.hkeys(key)
        vals = yield c.hvals(key)
        self.assertEqual(sorted(keys), sorted(h))
        self.assertEqual(sorted(vals), sorted(h.values()))
        yield eq(c.hlen(key), 3)
        yield eq(c.hmget(key, 'f1', 'f3', 'hj'),
                 {'f1': b'1', 'f3': b'foo', 'hj': None})
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.hkeys, key)
        yield self.async.assertRaises(ResponseError, c.hvals, key)
        yield self.async.assertRaises(ResponseError, c.hlen, key)
        yield self.async.assertRaises(ResponseError, c.hmget, key, 'f1', 'f2')

    def test_hsetnx(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.hsetnx(key, 'a', 'foo'), 1)
        yield eq(c.hget(key, 'a'), b'foo')
        yield eq(c.hsetnx(key, 'a', 'bla'), 0)
        yield eq(c.hget(key, 'a'), b'foo')
        yield self._remove_and_push(key)
        yield self.async.assertRaises(ResponseError, c.hsetnx, key, 'a', 'jk')

    ###########################################################################
    ##    LISTS
    def test_blpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.async.assertEqual
        c = self.client
        yield self.async.assertRaises(ResponseError, c.blpop, key1, 'bla')
        yield eq(c.rpush(key1, 1, 2), 2)
        yield eq(c.rpush(key2, 3, 4), 2)
        yield eq(c.blpop((key2, key1), 1), (bk2, b'3'))
        yield eq(c.blpop((key2, key1), 1), (bk2, b'4'))
        yield eq(c.blpop((key2, key1), 1), (bk1, b'1'))
        yield eq(c.blpop((key2, key1), 1), (bk1, b'2'))
        yield eq(c.blpop((key2, key1), 1), None)
        yield eq(c.rpush(key1, '1'), 1)
        yield eq(c.blpop(key1, 1), (bk1, b'1'))

    def test_brpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.rpush(key1, 1, 2), 2)
        yield eq(c.rpush(key2, 3, 4), 2)
        yield eq(c.brpop((key2, key1), 1), (bk2, b'4'))
        yield eq(c.brpop((key2, key1), 1), (bk2, b'3'))
        yield eq(c.brpop((key2, key1), 1), (bk1, b'2'))
        yield eq(c.brpop((key2, key1), 1), (bk1, b'1'))
        yield eq(c.brpop((key2, key1), 1), None)
        yield eq(c.rpush(key1, '1'), 1)
        yield eq(c.brpop(key1, 1), (bk1, b'1'))

    def test_lindex_llen(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.lindex(key, '0'), None)
        yield eq(c.llen(key), 0)
        yield eq(c.rpush(key, '1', '2', '3'), 3)
        yield eq(c.lindex(key, '0'), b'1')
        yield eq(c.lindex(key, '1'), b'2')
        yield eq(c.lindex(key, '2'), b'3')
        yield eq(c.lindex(key, '3'), None)
        yield eq(c.llen(key), 3)
        yield self._remove_and_sadd(key)
        yield self.async.assertRaises(ResponseError, c.lindex, key, '1')
        yield self.async.assertRaises(ResponseError, c.llen, key)

    def test_linsert(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield eq(c.linsert(key, 'after', '2', '2.5'), 0)
        yield eq(c.rpush(key, '1', '2', '3'), 3)
        yield eq(c.linsert(key, 'after', '2', '2.5'), 4)
        yield eq(c.lrange(key, 0, -1), [b'1', b'2', b'2.5', b'3'])
        yield eq(c.linsert(key, 'before', '2', '1.5'), 5)
        yield eq(c.lrange(key, 0, -1), [b'1', b'1.5', b'2', b'2.5', b'3'])
        yield eq(c.linsert(key, 'before', '100', '1.5'), -1)
        yield self.async.assertRaises(ResponseError, c.linsert, key,
                                      'banana', '2', '2.5')
        yield self._remove_and_sadd(key)
        yield self.async.assertRaises(ResponseError, c.linsert, key,
                                      'after', '2', '2.5')

    def test_lpop(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.lpop(key), None)
        yield eq(c.rpush(key, 1, 2), 2)
        yield eq(c.lpop(key), b'1')
        yield eq(c.lpop(key), b'2')
        yield eq(c.lpop(key), None)
        yield eq(c.type(key), 'none')
        yield self._remove_and_sadd(key, 0)
        yield self.async.assertRaises(ResponseError, c.lpop, key)
        yield self.async.assertRaises(ResponseError, c.lpush, key, 4)

    def test_rpop(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.rpop(key), None)
        yield eq(c.rpush(key, 1, 2), 2)
        yield eq(c.rpop(key), b'2')
        yield eq(c.rpop(key), b'1')
        yield eq(c.rpop(key), None)
        yield eq(c.type(key), 'none')

    def test_lpushx_rpushx(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.lpushx(key, 'b'), 0)
        yield eq(c.rpushx(key, 'b'), 0)
        yield eq(c.lrange(key, 0, -1), [])
        yield eq(c.lpush(key, 'a'), 1)
        yield eq(c.lpushx(key, 'b'), 2)
        yield eq(c.rpushx(key, 'c'), 3)
        yield eq(c.lrange(key, 0, -1), [b'b', b'a', b'c'])
        yield self._remove_and_sadd(key)
        yield self.async.assertRaises(ResponseError, c.lpushx, key, 'g')

    def test_lrem(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.lrem(key, 1, 'a'), 0)
        yield eq(c.rpush(key, 'a', 'a', 'a', 'b', 'a', 'a'), 6)
        yield eq(c.lrem(key, 1, 'a'), 1)
        yield eq(c.lrange(key, 0, -1), [b'a', b'a', b'b', b'a', b'a'])
        yield eq(c.lrem(key, -1, 'a'), 1)
        yield eq(c.lrange(key, 0, -1), [b'a', b'a', b'b', b'a'])
        yield eq(c.lrem(key, 0, 'a'), 3)
        yield self.async.assertRaises(ResponseError, c.lrem, key, 'g', 'foo')
        yield eq(c.lrange(key, 0, -1), [b'b'])
        yield eq(c.lrem(key, 0, 'b'), 1)
        yield self._remove_and_sadd(key, 0)
        yield self.async.assertRaises(ResponseError, c.lrem, key, 1)

    ###########################################################################
    ###     SORT
    def test_sort_basic(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield c.rpush(key, '3', '2', '1', '4')
        yield self.async.assertEqual(c.sort(key), [b'1', b'2', b'3', b'4'])

    def test_sort_limited(self):
        key = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        yield c.rpush(key, '3', '2', '1', '4')
        yield self.async.assertEqual(c.sort(key, start=1, num=2), [b'2', b'3'])

    def test_sort_by(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 8,
                  '%s:2' % key, 3,
                  '%s:3' % key, 5)
        pipe.rpush(key2, '3', '2', '1')
        res = yield pipe.commit()
        self.assertEqual(len(res), 2)
        yield eq(c.sort(key2, by='%s:*' % key), [b'2', b'3', b'1'])

    def test_sort_get(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 'u1',
                  '%s:2' % key, 'u2',
                  '%s:3' % key, 'u3')
        pipe.rpush(key2, '3', '2', '1')
        res = yield pipe.commit()
        self.assertEqual(len(res), 2)
        yield eq(c.sort(key2, get='%s:*' % key), [b'u1', b'u2', b'u3'])

    def test_sort_get_multi(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.async.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 'u1',
                  '%s:2' % key, 'u2',
                  '%s:3' % key, 'u3')
        pipe.rpush(key2, '3', '2', '1')
        res = yield pipe.commit()
        self.assertEqual(len(res), 2)
        yield eq(c.sort(key2, get=('%s:*' % key, '#')),
                 [b'u1', b'1', b'u2', b'2', b'u3', b'3'])
        yield eq(c.sort(key2, get=('%s:*' % key, '#'), groups=True),
                 [(b'u1', b'1'), (b'u2', b'2'), (b'u3', b'3')])

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

    def test_zrangebyscore(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield eq(c.zrangebyscore(key, 2, 4), [b'a2', b'a3', b'a4'])
        # slicing with start/num
        yield eq(c.zrangebyscore(key, 2, 4, offset=1, count=2),
                 [b'a3', b'a4'])
        # withscores
        yield eq(c.zrangebyscore(key, 2, 4, withscores=True),
                 Zset([(2.0, b'a2'), (3.0, b'a3'), (4.0, b'a4')]))

    def test_zrank(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield eq(c.zrank(key, 'a1'), 0)
        yield eq(c.zrank(key, 'a2'), 1)
        yield eq(c.zrank(key, 'a6'), None)

    def test_zrem(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield eq(c.zrem(key, 'a2'), 1)
        yield eq(c.zrange(key, 0, -1), [b'a1', b'a3', b'a4', b'a5'])
        yield eq(c.zrem(key, 'b'), 0)
        yield eq(c.zrange(key, 0, -1), [b'a1', b'a3', b'a4', b'a5'])
        yield eq(c.zrem(key, 'a3', 'a5', 'h'), 2)
        yield eq(c.zrange(key, 0, -1), [b'a1', b'a4'])
        yield eq(c.zrem(key, 'a1', 'a4'), 2)
        yield eq(c.type(key), 'none')

    def test_zremrangebyrank(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield eq(c.zremrangebyrank(key, 1, 3), 3)
        yield eq(c.zrange(key, 0, 5), [b'a1', b'a5'])

    def test_zremrangebyscore(self):
        key = self.randomkey()
        eq = self.async.assertEqual
        c = self.client
        yield eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield eq(c.zremrangebyscore(key, 2, 4), 3)
        yield eq(c.zrange(key, 0, -1), [b'a1', b'a5'])
        yield eq(c.zremrangebyscore(key, 2, 4), 0)
        yield eq(c.zrange(key, 0, -1), [b'a1', b'a5'])

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
        self.assertTrue(result >= 0)
        self.assertTrue(self.called)

    def test_pattern_subscribe(self):
        eq = self.async.assertEqual
        pubsub = self.client.pubsub(protocol=StringProtocol())
        listener = Listener()
        pubsub.add_client(listener)
        yield eq(pubsub.psubscribe('f*'), None)
        yield eq(pubsub.publish('foo', 'hello foo'), 1)
        channel, message = yield listener.get()
        self.assertEqual(channel, 'foo')
        self.assertEqual(message, 'hello foo')
        self.assertEqual(pubsub.punsubscribe(), None)
        #yield listener.get()

    ###########################################################################
    ##    TRANSACTION
    def test_watch(self):
        key1 = self.randomkey()
        key2 = key1 + '2'
        result = yield self.client.watch(key1)
        self.assertEqual(result, 1)

    ###########################################################################
    ##    SYNCHRONOUS CLIENT
    def test_sync(self):
        client = self.sync_store.client()
        self.assertFalse(client.store._loop.is_running())
        self.assertEqual(client.echo('Hello'), b'Hello')


class Scripting(object):

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


class TestPulsarStore(RedisCommands, unittest.TestCase):
    app_cfg = None

    @classmethod
    def setUpClass(cls):
        yield cls.create_pulsar_store()
        cls.sync_store = cls.create_store('%s/10' % cls.pulsards_uri,
                                          loop=new_event_loop())
        cls.client = cls.store.client()


@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestPulsarStorePyParser(TestPulsarStore):
    redis_py_parser = True
