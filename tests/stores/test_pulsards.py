import binascii
import time
import unittest
import asyncio
import datetime

import pulsar
from pulsar.utils.string import random_string
from pulsar.utils.structures import Zset
from pulsar.apps.ds import PulsarDS, redis_parser, ResponseError
from pulsar.apps.data import create_store


class Listener:

    def __init__(self):
        self._messages = asyncio.Queue()

    def __call__(self, channel, message):
        self._messages.put_nowait((channel, message))

    def get(self):
        return self._messages.get()


class StringProtocol:

    def encode(self, message):
        return message

    def decode(self, message):
        return message.decode('utf-8')


class StoreMixin:
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
        return random_string(min_length=length, max_length=length)

    @asyncio.coroutine
    def _remove_and_push(self, key, rem=1):
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.delete(key), rem)
        yield from eq(c.rpush(key, 'bla'), 1)

    @asyncio.coroutine
    def _remove_and_sadd(self, key, rem=1):
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.delete(key), rem)
        yield from eq(c.sadd(key, 'bla'), 1)


class RedisCommands(StoreMixin):

    def test_store(self):
        store = self.store
        self.assertTrue(store.namespace)

    ###########################################################################
    #    KEYS
    @asyncio.coroutine
    def test_dump_restore(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.dump(key), None)
        yield from eq(c.set(key, 'hello'), True)
        value = yield from c.dump(key)
        self.assertTrue(value)
        yield from self.wait.assertRaises(
            ResponseError, c.restore, key, 0, 'bla')
        yield from eq(c.restore(key+'2', 0, value), True)
        yield from eq(c.get(key+'2'), b'hello')

    @asyncio.coroutine
    def test_exists(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.exists(key), False)
        yield from eq(c.set(key, 'hello'), True)
        yield from eq(c.exists(key), True)
        yield from eq(c.delete(key), 1)

    @asyncio.coroutine
    def test_expire_persist_ttl(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from self.wait.assertRaises(ResponseError, c.expire, key, 'bla')
        yield from eq(c.expire(key, 1), False)
        yield from eq(c.set(key, 1), True)
        yield from eq(c.expire(key, 10), True)
        ttl = yield from c.ttl(key)
        self.assertTrue(ttl > 0 and ttl <= 10)
        yield from eq(c.persist(key), True)
        yield from eq(c.ttl(key), -1)
        yield from eq(c.persist(key), False)

    @asyncio.coroutine
    def test_expireat(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from self.wait.assertRaises(
            ResponseError, c.expireat, key, 'bla')
        t = int(time.time() + 3)
        yield from eq(c.expireat(key, t), False)
        yield from eq(c.set(key, 1), True)
        t = int(time.time() + 10)
        yield from eq(c.expireat(key, t), True)
        ttl = yield from c.ttl(key)
        self.assertTrue(ttl > 0 and ttl <= 10)
        yield from eq(c.persist(key), True)
        yield from eq(c.ttl(key), -1)
        yield from eq(c.persist(key), False)

    @asyncio.coroutine
    def test_keys(self):
        key = self.randomkey()
        keya = '%s_a' % key
        keyb = '%s_a' % key
        keyc = '%sc' % key
        c = self.client
        eq = self.wait.assertEqual
        keys_with_underscores = set([keya.encode('utf-8'),
                                     keyb.encode('utf-8')])
        keys = keys_with_underscores.union(set([keyc.encode('utf-8')]))
        yield from eq(c.mset(keya, 1, keyb, 2, keyc, 3), True)
        k1 = yield from c.keys('%s_*' % key)
        k2 = yield from c.keys('%s*' % key)
        self.assertEqual(set(k1), keys_with_underscores)
        self.assertEqual(set(k2), keys)

    @asyncio.coroutine
    def test_move(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        db = 3 if c.store.database == 4 else 4
        yield from self.wait.assertRaises(ResponseError, c.move, key, 'bla')
        yield from eq(c.move(key, db), False)
        yield from eq(c.set(key, 'ciao'), True)
        yield from eq(c.move(key, db), True)
        s2 = self.create_store(self.store.dns, database=db)
        c2 = s2.client()
        yield from eq(c2.get(key), b'ciao')
        yield from eq(c.exists(key), False)
        yield from eq(c.set(key, 'foo'), True)
        yield from eq(c.move(key, db), False)
        yield from eq(c.exists(key), True)

    @asyncio.coroutine
    def __test_randomkey(self):
        # TODO: this test fails sometimes
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.mset(key, 1, key+'a', 2, key+'b', 3), True)
        key = yield from c.randomkey()
        yield from eq(c.exists(key), True)

    @asyncio.coroutine
    def test_rename_renamenx(self):
        key = self.randomkey()
        des = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from self.wait.assertRaises(ResponseError, c.rename, key, des)
        yield from eq(c.set(key, 'hello'), True)
        yield from self.wait.assertRaises(ResponseError, c.rename, key, key)
        yield from eq(c.rename(key, des), True)
        yield from eq(c.exists(key), False)
        yield from eq(c.get(des), b'hello')
        yield from eq(c.set(key, 'ciao'), True)
        yield from eq(c.renamenx(key, des), False)
        yield from eq(c.renamenx(key, des+'a'), True)
        yield from eq(c.exists(key), False)

    ###########################################################################
    #    BAD REQUESTS
    # def test_no_command(self):
    #     yield from self.wait.assertRaises(ResponseError, self.store.execute)

    # def test_bad_command(self):
    #     yield from self.wait.assertRaises(ResponseError, self.store.execute,
    #                                   'foo')
    ###########################################################################
    #    STRINGS
    @asyncio.coroutine
    def test_append(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.append(key, 'a1'), 2)
        yield from eq(c.get(key), b'a1')
        yield from eq(c.append(key, 'a2'), 4)
        yield from eq(c.get(key), b'a1a2')
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.append, key, 'g')

    @asyncio.coroutine
    def test_bitcount(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.bitcount(key), 0)
        yield from eq(c.setbit(key, 5, 1), 0)
        yield from eq(c.bitcount(key), 1)
        yield from eq(c.setbit(key, 6, 1), 0)
        yield from eq(c.bitcount(key), 2)
        yield from eq(c.setbit(key, 5, 0), 1)
        yield from eq(c.bitcount(key), 1)
        yield from eq(c.setbit(key, 9, 1), 0)
        yield from eq(c.setbit(key, 17, 1), 0)
        yield from eq(c.setbit(key, 25, 1), 0)
        yield from eq(c.setbit(key, 33, 1), 0)
        yield from eq(c.bitcount(key), 5)
        yield from eq(c.bitcount(key, 0, -1), 5)
        yield from eq(c.bitcount(key, 2, 3), 2)
        yield from eq(c.bitcount(key, 2, -1), 3)
        yield from eq(c.bitcount(key, -2, -1), 2)
        yield from eq(c.bitcount(key, 1, 1), 1)
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.bitcount, key)

    @asyncio.coroutine
    def test_bitop_not_empty_string(self):
        key = self.randomkey()
        des = key + 'd'
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, ''), True)
        yield from eq(c.bitop('not', des, key), 0)
        yield from eq(c.get(des), None)

    @asyncio.coroutine
    def test_bitop_not(self):
        key = self.randomkey()
        des = key + 'd'
        c = self.client
        eq = self.wait.assertEqual
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        yield from eq(c.set(key, test_str), True)
        yield from eq(c.bitop('not', des, key), 4)
        result = yield from c.get(des)
        self.assertEqual(int(binascii.hexlify(result), 16), correct)

    @asyncio.coroutine
    def test_bitop_not_in_place(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        yield from eq(c.set(key, test_str), True)
        yield from eq(c.bitop('not', key, key), 4)
        result = yield from c.get(key)
        assert int(binascii.hexlify(result), 16) == correct

    @asyncio.coroutine
    def test_bitop_single_string(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        test_str = b'\x01\x02\xFF'
        yield from eq(c.set(key, test_str), True)
        yield from eq(c.bitop('and', key+'1', key), 3)
        yield from eq(c.bitop('or', key+'2', key), 3)
        yield from eq(c.bitop('xor', key+'3', key), 3)
        yield from eq(c.get(key + '1'), test_str)
        yield from eq(c.get(key + '2'), test_str)
        yield from eq(c.get(key + '3'), test_str)

    @asyncio.coroutine
    def test_bitop_string_operands(self):
        c = self.client
        eq = self.wait.assertEqual
        key1 = self.randomkey()
        key2 = key1 + '2'
        des1 = key1 + 'd1'
        des2 = key1 + 'd2'
        des3 = key1 + 'd3'
        yield from eq(c.set(key1, b'\x01\x02\xFF\xFF'), True)
        yield from eq(c.set(key2, b'\x01\x02\xFF'), True)
        yield from eq(c.bitop('and', des1, key1, key2), 4)
        yield from eq(c.bitop('or', des2, key1, key2), 4)
        yield from eq(c.bitop('xor', des3, key1, key2), 4)
        res1 = yield from c.get(des1)
        res2 = yield from c.get(des2)
        res3 = yield from c.get(des3)
        self.assertEqual(int(binascii.hexlify(res1), 16), 0x0102FF00)
        self.assertEqual(int(binascii.hexlify(res2), 16), 0x0102FFFF)
        self.assertEqual(int(binascii.hexlify(res3), 16), 0x000000FF)

    @asyncio.coroutine
    def test_decr(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.decr(key), -1)
        yield from eq(c.get(key), b'-1')
        yield from eq(c.decr(key), -2)
        yield from eq(c.get(key), b'-2')
        yield from eq(c.decr(key, 5), -7)
        yield from eq(c.get(key), b'-7')

    @asyncio.coroutine
    def test_getbit(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.getbit(key, 5), 0)
        yield from eq(c.setbit(key, 5, 1), 0)
        yield from eq(c.getbit(key, 5), 1)
        yield from self.wait.assertRaises(ResponseError, c.getbit, key, -1)
        yield from eq(c.getbit(key, 4), 0)
        yield from eq(c.setbit(key, 4, 1), 0)
        # set bit 4
        yield from eq(c.getbit(key, 4), 1)
        yield from eq(c.getbit(key, 5), 1)
        # set bit 5 again
        yield from eq(c.setbit(key, 5, 1), 1)
        yield from eq(c.getbit(key, 5), 1)
        #
        yield from eq(c.getbit(key, 30), 0)
        #
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.getbit, key, 1)

    @asyncio.coroutine
    def test_getrange(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.getrange(key, 0, 0), b'')
        yield from eq(c.set(key, 'Hello there'), True)
        yield from eq(c.getrange(key, 0, 0), b'H')
        yield from eq(c.getrange(key, 0, 4), b'Hello')
        yield from eq(c.getrange(key, 5, 5), b' ')
        yield from eq(c.getrange(key, 20, 25), b'')
        yield from eq(c.getrange(key, -5, -1), b'there')
        yield from self.wait.assertRaises(
            ResponseError, c.getrange, key, 1, 'b')
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(
            ResponseError, c.getrange, key, 1, 2)

    @asyncio.coroutine
    def test_getset(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.getset(key, 'foo'), None)
        yield from eq(c.getset(key, 'bar'), b'foo')
        yield from eq(c.get(key), b'bar')

    @asyncio.coroutine
    def test_get(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from c.set(key, 'foo')
        yield from eq(c.get(key), b'foo')
        yield from eq(c.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'), None)

    @asyncio.coroutine
    def test_incr(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.incr(key), 1)
        yield from eq(c.get(key), b'1')
        yield from eq(c.incr(key), 2)
        yield from eq(c.get(key), b'2')
        yield from eq(c.incr(key, 5), 7)
        yield from eq(c.get(key), b'7')

    @asyncio.coroutine
    def test_incrby(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.incrby(key), 1)
        yield from eq(c.incrby(key, 4), 5)
        yield from eq(c.get(key), b'5')

    @asyncio.coroutine
    def test_incrbyfloat(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.incrbyfloat(key), 1.0)
        yield from eq(c.get(key), b'1')
        yield from eq(c.incrbyfloat(key, 1.1), 2.1)
        yield from eq(c.get(key), b'2.1')

    @asyncio.coroutine
    def test_mget(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        key3 = key2 + 'y'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.set(key1, 'foox'), True)
        yield from eq(c.set(key2, 'fooxx'), True)
        yield from eq(c.mget(key1, key2, key3), [b'foox', b'fooxx', None])

    @asyncio.coroutine
    def test_msetnx(self):
        key1 = self.randomkey()
        key2 = self.randomkey()
        key3 = self.randomkey()
        key4 = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.msetnx(key1, '1', key2, '2', key3, '3'), True)
        yield from eq(c.msetnx(key1, 'x', key4, 'y'), False)
        values = yield from c.mget(key1, key2, key3, key4)
        self.assertEqual(len(values), 4)
        for value, target in zip(values, (b'1', b'2', b'3', None)):
            self.assertEqual(value, target)

    @asyncio.coroutine
    def test_set_ex(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, '1', ex=5), True)
        ttl = yield from c.ttl(key)
        self.assertTrue(0 < ttl <= 5)

    @asyncio.coroutine
    def test_set_ex_timedelta(self):
        expire_at = datetime.timedelta(seconds=5)
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, '1', ex=expire_at), True)
        ttl = yield from c.ttl(key)
        self.assertTrue(0 < ttl <= 5)

    @asyncio.coroutine
    def test_set_nx(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, '1', nx=True), True)
        yield from eq(c.set(key, '2', nx=True), False)
        yield from eq(c.get(key), b'1')

    @asyncio.coroutine
    def test_set_px(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, '1', px=10000, nx=True), True)
        yield from eq(c.get(key), b'1')
        pttl = yield from c.pttl(key)
        self.assertTrue(0 < pttl <= 10000)
        ttl = yield from c.ttl(key)
        self.assertTrue(0 < ttl <= 10)

    @asyncio.coroutine
    def test_set_px_timedelta(self):
        expire_at = datetime.timedelta(milliseconds=5000)
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, '1', px=expire_at, nx=True), True)
        yield from eq(c.get(key), b'1')
        pttl = yield from c.pttl(key)
        self.assertTrue(0 < pttl <= 5000)
        ttl = yield from c.ttl(key)
        self.assertTrue(0 < ttl <= 5)

    @asyncio.coroutine
    def test_set_xx(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.set(key, '1', xx=True), False)
        yield from eq(c.get(key), None)
        yield from eq(c.set(key, 'bar'), True)
        yield from eq(c.set(key, '2', xx=True), True)
        yield from eq(c.get(key), b'2')

    @asyncio.coroutine
    def test_setrange(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.setrange(key, 5, 'foo'), 8)
        yield from eq(c.get(key), b'\0\0\0\0\0foo')
        yield from eq(c.set(key, 'abcdefghijh'), True)
        yield from eq(c.setrange(key, 6, '12345'), 11)
        yield from eq(c.get(key), b'abcdef12345')

    @asyncio.coroutine
    def test_strlen(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.strlen(key), 0)
        yield from eq(c.set(key, 'foo'), True)
        yield from eq(c.strlen(key), 3)

    ###########################################################################
    #    HASHES
    @asyncio.coroutine
    def test_hdel(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.hdel(key, 'f1', 'f2', 'gh'), 0)
        yield from eq(c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}),
                      True)
        yield from eq(c.hdel(key, 'f1', 'f2', 'gh'), 2)
        yield from eq(c.hdel(key, 'fgf'), 0)
        yield from eq(c.type(key), 'hash')
        yield from eq(c.hdel(key, 'f3'), 1)
        yield from eq(c.type(key), 'none')
        yield from self._remove_and_push(key, 0)
        yield from self.wait.assertRaises(ResponseError, c.hdel, key, 'foo')

    @asyncio.coroutine
    def test_hexists(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.hexists(key, 'foo'), False)
        yield from eq(c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}),
                      True)
        yield from eq(c.hexists(key, 'f3'), True)
        yield from eq(c.hexists(key, 'f5'), False)
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.hexists, key,
                                          'foo')

    @asyncio.coroutine
    def test_hset_hget(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.hget(key, 'foo'), None)
        yield from eq(c.hset(key, 'foo', 4), 1)
        yield from eq(c.hget(key, 'foo'), b'4')
        yield from eq(c.hset(key, 'foo', 6), 0)
        yield from eq(c.hget(key, 'foo'), b'6')
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(
            ResponseError, c.hset, key, 'foo', 7)
        yield from self.wait.assertRaises(ResponseError, c.hget, key, 'foo')
        yield from self.wait.assertRaises(ResponseError, c.hmset, key, 'foo')

    @asyncio.coroutine
    def test_hgetall(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        h = {b'f1': b'1', b'f2': b'hello', b'f3': b'foo'}
        yield from eq(c.hgetall(key), {})
        yield from eq(c.hmset(key, h), True)
        yield from eq(c.hgetall(key), h)
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.hgetall, key)

    @asyncio.coroutine
    def test_hincrby(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.hincrby(key, 'foo', 1), 1)
        yield from eq(c.hincrby(key, 'foo', 2), 3)
        yield from eq(c.hincrby(key, 'foo', -1), 2)
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(
            ResponseError, c.hincrby, key, 'foo', 3)

    @asyncio.coroutine
    def test_hincrbyfloat(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.hincrbyfloat(key, 'foo', 1), 1.0)
        yield from eq(c.hincrbyfloat(key, 'foo', 2.5), 3.5)
        yield from eq(c.hincrbyfloat(key, 'foo', -1.1), 2.4)
        yield from self._remove_and_push(key)

    @asyncio.coroutine
    def test_hkeys_hvals_hlen_hmget(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        h = {b'f1': b'1', b'f2': b'hello', b'f3': b'foo'}
        yield from eq(c.hkeys(key), [])
        yield from eq(c.hvals(key), [])
        yield from eq(c.hlen(key), 0)
        yield from eq(c.hmset(key, h), True)
        keys = yield from c.hkeys(key)
        vals = yield from c.hvals(key)
        self.assertEqual(sorted(keys), sorted(h))
        self.assertEqual(sorted(vals), sorted(h.values()))
        yield from eq(c.hlen(key), 3)
        yield from eq(c.hmget(key, 'f1', 'f3', 'hj'),
                      {'f1': b'1', 'f3': b'foo', 'hj': None})
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.hkeys, key)
        yield from self.wait.assertRaises(ResponseError, c.hvals, key)
        yield from self.wait.assertRaises(ResponseError, c.hlen, key)
        yield from self.wait.assertRaises(ResponseError, c.hmget, key,
                                          'f1', 'f2')

    @asyncio.coroutine
    def test_hsetnx(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.hsetnx(key, 'a', 'foo'), 1)
        yield from eq(c.hget(key, 'a'), b'foo')
        yield from eq(c.hsetnx(key, 'a', 'bla'), 0)
        yield from eq(c.hget(key, 'a'), b'foo')
        yield from self._remove_and_push(key)
        yield from self.wait.assertRaises(ResponseError, c.hsetnx, key,
                                          'a', 'jk')

    ###########################################################################
    #    LISTS
    @asyncio.coroutine
    def test_blpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.wait.assertEqual
        c = self.client
        yield from self.wait.assertRaises(ResponseError, c.blpop, key1, 'bla')
        yield from eq(c.rpush(key1, 1, 2), 2)
        yield from eq(c.rpush(key2, 3, 4), 2)
        yield from eq(c.blpop((key2, key1), 1), (bk2, b'3'))
        yield from eq(c.blpop((key2, key1), 1), (bk2, b'4'))
        yield from eq(c.blpop((key2, key1), 1), (bk1, b'1'))
        yield from eq(c.blpop((key2, key1), 1), (bk1, b'2'))
        yield from eq(c.blpop((key2, key1), 1), None)
        yield from eq(c.rpush(key1, '1'), 1)
        yield from eq(c.blpop(key1, 1), (bk1, b'1'))

    @asyncio.coroutine
    def test_brpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.rpush(key1, 1, 2), 2)
        yield from eq(c.rpush(key2, 3, 4), 2)
        yield from eq(c.brpop((key2, key1), 1), (bk2, b'4'))
        yield from eq(c.brpop((key2, key1), 1), (bk2, b'3'))
        yield from eq(c.brpop((key2, key1), 1), (bk1, b'2'))
        yield from eq(c.brpop((key2, key1), 1), (bk1, b'1'))
        yield from eq(c.brpop((key2, key1), 1), None)
        yield from eq(c.rpush(key1, '1'), 1)
        yield from eq(c.brpop(key1, 1), (bk1, b'1'))

    @asyncio.coroutine
    def test_brpoplpush(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.rpush(key1, 1, 2), 2)
        yield from eq(c.rpush(key2, 3, 4), 2)
        yield from eq(c.brpoplpush(key1, key2), b'2')
        yield from eq(c.brpoplpush(key1, key2), b'1')
        yield from eq(c.brpoplpush(key1, key2, timeout=1), None)
        yield from eq(c.lrange(key1, 0, -1), [])
        yield from eq(c.lrange(key2, 0, -1), [b'1', b'2', b'3', b'4'])

    @asyncio.coroutine
    def test_brpoplpush_empty_string(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.rpush(key1, ''), 1)
        yield from eq(c.brpoplpush(key1, key2), b'')

    @asyncio.coroutine
    def test_lindex_llen(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.lindex(key, '0'), None)
        yield from eq(c.llen(key), 0)
        yield from eq(c.rpush(key, '1', '2', '3'), 3)
        yield from eq(c.lindex(key, '0'), b'1')
        yield from eq(c.lindex(key, '1'), b'2')
        yield from eq(c.lindex(key, '2'), b'3')
        yield from eq(c.lindex(key, '3'), None)
        yield from eq(c.llen(key), 3)
        yield from self._remove_and_sadd(key)
        yield from self.wait.assertRaises(ResponseError, c.lindex, key, '1')
        yield from self.wait.assertRaises(ResponseError, c.llen, key)

    @asyncio.coroutine
    def test_linsert(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.linsert(key, 'after', '2', '2.5'), 0)
        yield from eq(c.rpush(key, '1', '2', '3'), 3)
        yield from eq(c.linsert(key, 'after', '2', '2.5'), 4)
        yield from eq(c.lrange(key, 0, -1), [b'1', b'2', b'2.5', b'3'])
        yield from eq(c.linsert(key, 'before', '2', '1.5'), 5)
        yield from eq(c.lrange(key, 0, -1), [b'1', b'1.5', b'2', b'2.5', b'3'])
        yield from eq(c.linsert(key, 'before', '100', '1.5'), -1)
        yield from self.wait.assertRaises(ResponseError, c.linsert, key,
                                          'banana', '2', '2.5')
        yield from self._remove_and_sadd(key)
        yield from self.wait.assertRaises(ResponseError, c.linsert, key,
                                          'after', '2', '2.5')

    @asyncio.coroutine
    def test_lpop(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.lpop(key), None)
        yield from eq(c.rpush(key, 1, 2), 2)
        yield from eq(c.lpop(key), b'1')
        yield from eq(c.lpop(key), b'2')
        yield from eq(c.lpop(key), None)
        yield from eq(c.type(key), 'none')
        yield from self._remove_and_sadd(key, 0)
        yield from self.wait.assertRaises(ResponseError, c.lpop, key)
        yield from self.wait.assertRaises(ResponseError, c.lpush, key, 4)

    @asyncio.coroutine
    def test_lset(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.rpush(key, '1', '2', '3'), 3)
        yield from eq(c.lrange(key, 0, -1), [b'1', b'2', b'3'])
        yield from eq(c.lset(key, 1, '4'), True)
        yield from eq(c.lrange(key, 0, 2), [b'1', b'4', b'3'])

    @asyncio.coroutine
    def test_ltrim(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from eq(c.rpush(key, '1', '2', '3'), 3)
        yield from eq(c.ltrim(key, 0, 1), True)
        yield from eq(c.lrange(key, 0, -1), [b'1', b'2'])

    @asyncio.coroutine
    def test_lpushx_rpushx(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.lpushx(key, 'b'), 0)
        yield from eq(c.rpushx(key, 'b'), 0)
        yield from eq(c.lrange(key, 0, -1), [])
        yield from eq(c.lpush(key, 'a'), 1)
        yield from eq(c.lpushx(key, 'b'), 2)
        yield from eq(c.rpushx(key, 'c'), 3)
        yield from eq(c.lrange(key, 0, -1), [b'b', b'a', b'c'])
        yield from self._remove_and_sadd(key)
        yield from self.wait.assertRaises(ResponseError, c.lpushx, key, 'g')

    @asyncio.coroutine
    def test_lrem(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.lrem(key, 1, 'a'), 0)
        yield from eq(c.rpush(key, 'a', 'a', 'a', 'b', 'a', 'a'), 6)
        yield from eq(c.lrem(key, 1, 'a'), 1)
        yield from eq(c.lrange(key, 0, -1), [b'a', b'a', b'b', b'a', b'a'])
        yield from eq(c.lrem(key, -1, 'a'), 1)
        yield from eq(c.lrange(key, 0, -1), [b'a', b'a', b'b', b'a'])
        yield from eq(c.lrem(key, 0, 'a'), 3)
        yield from self.wait.assertRaises(ResponseError, c.lrem, key,
                                          'g', 'foo')
        yield from eq(c.lrange(key, 0, -1), [b'b'])
        yield from eq(c.lrem(key, 0, 'b'), 1)
        yield from self._remove_and_sadd(key, 0)
        yield from self.wait.assertRaises(ResponseError, c.lrem, key, 1)

    @asyncio.coroutine
    def test_rpop(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.rpop(key), None)
        yield from eq(c.rpush(key, 1, 2), 2)
        yield from eq(c.rpop(key), b'2')
        yield from eq(c.rpop(key), b'1')
        yield from eq(c.rpop(key), None)
        yield from eq(c.type(key), 'none')

    @asyncio.coroutine
    def test_rpoplpush(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.rpush(key1, 'a1', 'a2', 'a3'), 3)
        yield from eq(c.rpush(key2, 'b1', 'b2', 'b3'), 3)
        yield from eq(c.rpoplpush(key1, key2), b'a3')
        yield from eq(c.lrange(key1, 0, -1), [b'a1', b'a2'])
        yield from eq(c.lrange(key2, 0, -1), [b'a3', b'b1', b'b2', b'b3'])

    ###########################################################################
    #    SORT
    @asyncio.coroutine
    def test_sort_basic(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from c.rpush(key, '3', '2', '1', '4')
        yield from eq(c.sort(key), [b'1', b'2', b'3', b'4'])

    @asyncio.coroutine
    def test_sort_limited(self):
        key = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        yield from c.rpush(key, '3', '2', '1', '4')
        yield from eq(c.sort(key, start=1, num=2), [b'2', b'3'])

    @asyncio.coroutine
    def test_sort_by(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 8,
                  '%s:2' % key, 3,
                  '%s:3' % key, 5)
        pipe.rpush(key2, '3', '2', '1')
        res = yield from pipe.commit()
        self.assertEqual(len(res), 2)
        yield from eq(c.sort(key2, by='%s:*' % key), [b'2', b'3', b'1'])

    @asyncio.coroutine
    def test_sort_get(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 'u1',
                  '%s:2' % key, 'u2',
                  '%s:3' % key, 'u3')
        pipe.rpush(key2, '3', '2', '1')
        res = yield from pipe.commit()
        self.assertEqual(len(res), 2)
        yield from eq(c.sort(key2, get='%s:*' % key), [b'u1', b'u2', b'u3'])

    @asyncio.coroutine
    def test_sort_get_multi(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.wait.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 'u1',
                  '%s:2' % key, 'u2',
                  '%s:3' % key, 'u3')
        pipe.rpush(key2, '3', '2', '1')
        res = yield from pipe.commit()
        self.assertEqual(len(res), 2)
        yield from eq(c.sort(key2, get=('%s:*' % key, '#')),
                      [b'u1', b'1', b'u2', b'2', b'u3', b'3'])
        yield from eq(c.sort(key2, get=('%s:*' % key, '#'), groups=True),
                      [(b'u1', b'1'), (b'u2', b'2'), (b'u3', b'3')])

    ###########################################################################
    #    SETS
    @asyncio.coroutine
    def test_sadd_scard(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        members = (b'1', b'2', b'3', b'2')
        yield from eq(c.sadd(key, *members), 3)
        yield from eq(c.smembers(key), set(members))
        yield from eq(c.scard(key), 3)

    @asyncio.coroutine
    def test_sdiff(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sdiff(key, key2), set((b'1', b'2', b'3')))
        yield from eq(c.sadd(key2, 2, 3), 2)
        yield from eq(c.sdiff(key, key2), set([b'1']))

    @asyncio.coroutine
    def test_sdiffstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sdiffstore(des, key, key2), 3)
        yield from eq(c.smembers(des), set([b'1', b'2', b'3']))
        yield from eq(c.sadd(key2, 2, 3), 2)
        yield from eq(c.sdiffstore(des, key, key2), 1)
        yield from eq(c.smembers(des), set([b'1']))

    @asyncio.coroutine
    def test_sinter(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sinter(key, key2), set())
        yield from eq(c.sadd(key2, 2, 3), 2)
        yield from eq(c.sinter(key, key2), set([b'2', b'3']))

    @asyncio.coroutine
    def test_sinterstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sinterstore(des, key, key2), 0)
        yield from eq(c.smembers(des), set())
        yield from eq(c.sadd(key2, 2, 3), 2)
        yield from eq(c.sinterstore(des, key, key2), 2)
        yield from eq(c.smembers(des), set([b'2', b'3']))

    @asyncio.coroutine
    def test_sismember(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sismember(key, 1), True)
        yield from eq(c.sismember(key, 2), True)
        yield from eq(c.sismember(key, 3), True)
        yield from eq(c.sismember(key, 4), False)

    @asyncio.coroutine
    def test_smove(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.smove(key, key2, 1), False)
        yield from eq(c.sadd(key, 1, 2), 2)
        yield from eq(c.sadd(key2, 3, 4), 2)
        yield from eq(c.smove(key, key2, 1), True)
        yield from eq(c.smembers(key), set([b'2']))
        yield from eq(c.smembers(key2), set([b'1', b'3', b'4']))

    @asyncio.coroutine
    def test_spop(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        value = yield from c.spop(key)
        self.assertTrue(value in set([b'1', b'2', b'3']))
        yield from eq(c.smembers(key), set([b'1', b'2', b'3']) - set([value]))

    @asyncio.coroutine
    def test_srandmember(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        value = yield from c.srandmember(key)
        self.assertTrue(value in set((b'1', b'2', b'3')))
        yield from eq(c.smembers(key), set((b'1', b'2', b'3')))

    @asyncio.coroutine
    def test_srandmember_multi_value(self):
        s = [b'1', b'2', b'3']
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, *s), 3)
        randoms = yield from c.srandmember(key, 2)
        self.assertEqual(len(randoms), 2)
        self.assertEqual(set(randoms).intersection(s), set(randoms))

    @asyncio.coroutine
    def test_srem(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3, 4), 4)
        yield from eq(c.srem(key, 5), 0)
        yield from eq(c.srem(key, 5), 0)
        yield from eq(c.srem(key, 2, 4), 2)
        yield from eq(c.smembers(key), set([b'1', b'3']))

    @asyncio.coroutine
    def test_sunion(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sunion(key, key2), set((b'1', b'2', b'3')))
        yield from eq(c.sadd(key2, 2, 3, 4), 3)
        yield from eq(c.sunion(key, key2), set((b'1', b'2', b'3', b'4')))

    @asyncio.coroutine
    def test_sunionstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.sadd(key, 1, 2, 3), 3)
        yield from eq(c.sunionstore(des, key, key2), 3)
        yield from eq(c.smembers(des), set([b'1', b'2', b'3']))
        yield from eq(c.sadd(key2, 2, 3, 4), 3)
        yield from eq(c.sunionstore(des, key, key2), 4)
        yield from eq(c.smembers(des), set([b'1', b'2', b'3', b'4']))

    ###########################################################################
    #    SORTED SETS
    @asyncio.coroutine
    def test_zadd_zcard(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield from eq(c.zrange(key, 0, -1), [b'a1', b'a2', b'a3'])
        yield from eq(c.zcard(key), 3)

    @asyncio.coroutine
    def test_zcount(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield from eq(c.zcount(key, '-inf', '+inf'), 3)
        yield from eq(c.zcount(key, '(1', 2), 1)
        yield from eq(c.zcount(key, '(1', '(3'), 1)
        yield from eq(c.zcount(key, 1, 3), 3)

    @asyncio.coroutine
    def test_zincrby(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield from eq(c.zincrby(key, 1, 'a2'), 3.0)
        yield from eq(c.zincrby(key, 5, 'a3'), 8.0)
        yield from eq(c.zscore(key, 'a2'), 3.0)
        yield from eq(c.zscore(key, 'a3'), 8.0)
        yield from eq(c.zscore(key, 'blaaa'), None)

    @asyncio.coroutine
    def test_zinterstore_sum(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield from eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield from eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield from eq(c.zinterstore(des, (key1, key2, key3)), 2)
        yield from eq(c.zrange(des, 0, -1, withscores=True),
                      Zset(((8.0, b'a3'), (9.0, b'a1'))))

    @asyncio.coroutine
    def test_zinterstore_max(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield from eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield from eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield from eq(c.zinterstore(des, (key1, key2, key3),
                                    aggregate='max'), 2)
        yield from eq(c.zrange(des, 0, -1, withscores=True),
                      Zset(((5.0, b'a3'), (6.0, b'a1'))))

    @asyncio.coroutine
    def test_zinterstore_min(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield from eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield from eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield from eq(c.zinterstore(des, (key1, key2, key3),
                                    aggregate='min'), 2)
        yield from eq(c.zrange(des, 0, -1, withscores=True),
                      Zset(((1.0, b'a3'), (1.0, b'a1'))))

    @asyncio.coroutine
    def test_zinterstore_with_weights(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key1, a1=1, a2=2, a3=1), 3)
        yield from eq(c.zadd(key2, a1=2, a2=2, a3=2), 3)
        yield from eq(c.zadd(key3, a1=6, a3=5, a4=4), 3)
        yield from eq(c.zinterstore(des, (key1, key2, key3),
                                    weights=(1, 2, 3)), 2)
        yield from eq(c.zrange(des, 0, -1, withscores=True),
                      Zset(((20.0, b'a3'), (23.0, b'a1'))))

    @asyncio.coroutine
    def test_zrange(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3), 3)
        yield from eq(c.zrange(key, 0, 1), [b'a1', b'a2'])
        yield from eq(c.zrange(key, 1, 2), [b'a2', b'a3'])
        yield from eq(c.zrange(key, 0, 1, withscores=True),
                      Zset([(1, b'a1'), (2, b'a2')]))
        yield from eq(c.zrange(key, 1, 2, withscores=True),
                      Zset([(2, b'a2'), (3, b'a3')]))

    @asyncio.coroutine
    def test_zrangebyscore(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield from eq(c.zrangebyscore(key, 2, 4), [b'a2', b'a3', b'a4'])
        # slicing with start/num
        yield from eq(c.zrangebyscore(key, 2, 4, offset=1, count=2),
                      [b'a3', b'a4'])
        # withscores
        yield from eq(c.zrangebyscore(key, 2, 4, withscores=True),
                      Zset([(2.0, b'a2'), (3.0, b'a3'), (4.0, b'a4')]))

    @asyncio.coroutine
    def test_zrank(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield from eq(c.zrank(key, 'a1'), 0)
        yield from eq(c.zrank(key, 'a2'), 1)
        yield from eq(c.zrank(key, 'a6'), None)

    @asyncio.coroutine
    def test_zrem(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield from eq(c.zrem(key, 'a2'), 1)
        yield from eq(c.zrange(key, 0, -1), [b'a1', b'a3', b'a4', b'a5'])
        yield from eq(c.zrem(key, 'b'), 0)
        yield from eq(c.zrange(key, 0, -1), [b'a1', b'a3', b'a4', b'a5'])
        yield from eq(c.zrem(key, 'a3', 'a5', 'h'), 2)
        yield from eq(c.zrange(key, 0, -1), [b'a1', b'a4'])
        yield from eq(c.zrem(key, 'a1', 'a4'), 2)
        yield from eq(c.type(key), 'none')

    @asyncio.coroutine
    def test_zremrangebyrank(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield from eq(c.zremrangebyrank(key, 1, 3), 3)
        yield from eq(c.zrange(key, 0, 5), [b'a1', b'a5'])

    @asyncio.coroutine
    def test_zremrangebyscore(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        c = self.client
        yield from eq(c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        yield from eq(c.zremrangebyscore(key, 2, 4), 3)
        yield from eq(c.zrange(key, 0, -1), [b'a1', b'a5'])
        yield from eq(c.zremrangebyscore(key, 2, 4), 0)
        yield from eq(c.zrange(key, 0, -1), [b'a1', b'a5'])

    ###########################################################################
    #    CONNECTION
    @asyncio.coroutine
    def test_ping(self):
        result = yield from self.client.ping()
        self.assertTrue(result)

    @asyncio.coroutine
    def test_echo(self):
        result = yield from self.client.echo('Hello')
        self.assertEqual(result, b'Hello')

    ###########################################################################
    #    SERVER
    @asyncio.coroutine
    def test_dbsize(self):
        yield from self.client.set('one_at_least', 'foo')
        result = yield from self.client.dbsize()
        self.assertTrue(result >= 1)

    @asyncio.coroutine
    def test_info(self):
        info = yield from self.client.info()
        self.assertTrue(info)
        self.assertIsInstance(info, dict)

    @asyncio.coroutine
    def test_time(self):
        t = yield from self.client.time()
        self.assertIsInstance(t, tuple)
        total = t[0] + 0.000001*t[1]
        self.assertTrue(total)

    ###########################################################################
    #    PUBSUB
    def test_handler(self):
        client = self.client
        pubsub = client.pubsub()
        self.assertEqual(client.store, pubsub.store)
        self.assertEqual(client.store._loop, pubsub._loop)
        self.assertEqual(pubsub._connection, None)

    @asyncio.coroutine
    def test_subscribe_one(self):
        key = self.randomkey()
        pubsub1 = self.client.pubsub()
        self.assertFalse(pubsub1._connection)
        # Subscribe to one channel
        yield from pubsub1.subscribe(key)
        count = yield from pubsub1.count(key)
        self.assertEqual(len(count), 1)
        self.assertEqual(count[key.encode('utf-8')], 1)
        #
        pubsub2 = self.client.pubsub()
        yield from pubsub2.subscribe(key)
        count = yield from pubsub1.count(key)
        self.assertEqual(len(count), 1)
        self.assertEqual(count[key.encode('utf-8')], 2)

    @asyncio.coroutine
    def test_subscribe_many(self):
        base = self.randomkey()
        key1 = base + '_a'
        key2 = base + '_b'
        key3 = base + '_c'
        key4 = base + 'x'
        pubsub = self.client.pubsub()
        yield from pubsub.subscribe(key1, key2, key3, key4)
        channels = yield from pubsub.channels(base + '_*')
        self.assertEqual(len(channels), 3)
        count = yield from pubsub.count(key1, key2, key3)
        self.assertEqual(len(count), 3)
        self.assertEqual(count[key1.encode('utf-8')], 1)
        self.assertEqual(count[key2.encode('utf-8')], 1)
        self.assertEqual(count[key3.encode('utf-8')], 1)

    @asyncio.coroutine
    def test_publish(self):
        pubsub = self.client.pubsub()
        listener = Listener()
        pubsub.add_client(listener)
        yield from pubsub.subscribe('chat')
        result = yield from pubsub.publish('chat', 'Hello')
        self.assertTrue(result >= 0)
        channel, message = yield from listener.get()
        self.assertEqual(channel, 'chat')
        self.assertEqual(message, b'Hello')

    @asyncio.coroutine
    def test_pattern_subscribe(self):
        # switched off for redis. Issue #95
        if self.store.name == 'pulsar':
            eq = self.wait.assertEqual
            pubsub = self.client.pubsub(protocol=StringProtocol())
            listener = Listener()
            pubsub.add_client(listener)
            yield from eq(pubsub.psubscribe('f*'), None)
            yield from eq(pubsub.publish('foo', 'hello foo'), 1)
            channel, message = yield from listener.get()
            self.assertEqual(channel, 'foo')
            self.assertEqual(message, 'hello foo')
            yield from eq(pubsub.punsubscribe(), None)
            # yield from listener.get()

    ###########################################################################
    #    TRANSACTION
    @asyncio.coroutine
    def test_watch(self):
        key1 = self.randomkey()
        # key2 = key1 + '2'
        result = yield from self.client.watch(key1)
        self.assertEqual(result, 1)


class TestPulsarStore(RedisCommands, unittest.TestCase):
    app_cfg = None

    @classmethod
    @asyncio.coroutine
    def setUpClass(cls):
        server = PulsarDS(name=cls.__name__.lower(),
                          bind='127.0.0.1:0',
                          concurrency=cls.cfg.concurrency,
                          redis_py_parser=cls.redis_py_parser)
        cls.app_cfg = yield from pulsar.send('arbiter', 'run', server)
        cls.pulsards_uri = 'pulsar://%s:%s' % cls.app_cfg.addresses[0]
        cls.store = cls.create_store('%s/9' % cls.pulsards_uri)
        cls.client = cls.store.client()

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return pulsar.send('arbiter', 'kill_actor', cls.app_cfg.name)

    def test_store_methods(self):
        store = self.create_store('%s/8' % self.pulsards_uri)
        self.assertEqual(store.database, 8)
        store.database = 10
        self.assertEqual(store.database, 10)
        self.assertTrue(store.dns.startswith('%s/10?' % self.pulsards_uri))
        self.assertEqual(store.encoding, 'utf-8')
        self.assertTrue(repr(store))


@unittest.skipUnless(pulsar.HAS_C_EXTENSIONS, 'Requires cython extensions')
class TestPulsarStorePyParser(TestPulsarStore):
    redis_py_parser = True
