import binascii
import time
import json
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


class JsonProtocol:

    def encode(self, message):
        return json.dumps(message)

    def decode(self, message):
        return json.loads(message.decode('utf-8'))


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

    async def _remove_and_push(self, key, rem=1):
        c = self.client
        self.assertEqual(await c.delete(key), rem)
        self.assertEqual(await c.rpush(key, 'bla'), 1)

    async def _remove_and_sadd(self, key, rem=1):
        c = self.client
        self.assertEqual(await c.delete(key), rem)
        self.assertEqual(await c.sadd(key, 'bla'), 1)


class RedisCommands(StoreMixin):

    def test_store(self):
        store = self.store
        self.assertTrue(store.namespace)

    ###########################################################################
    #    KEYS
    async def test_dump_restore(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.dump(key), None)
        eq(await c.set(key, 'hello'), True)
        value = await c.dump(key)
        self.assertTrue(value)
        await self.wait.assertRaises(
            ResponseError, c.restore, key, 0, 'bla')
        eq(await c.restore(key+'2', 0, value), True)
        eq(await c.get(key+'2'), b'hello')

    async def test_exists(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.exists(key), False)
        eq(await c.set(key, 'hello'), True)
        eq(await c.exists(key), True)
        eq(await c.delete(key), 1)

    async def test_expire_persist_ttl(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        await self.wait.assertRaises(ResponseError, c.expire, key, 'bla')
        eq(await c.expire(key, 1), False)
        eq(await c.set(key, 1), True)
        eq(await c.expire(key, 10), True)
        ttl = await c.ttl(key)
        self.assertTrue(ttl > 0 and ttl <= 10)
        eq(await c.persist(key), True)
        eq(await c.ttl(key), -1)
        eq(await c.persist(key), False)

    async def test_expireat(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        await self.wait.assertRaises(ResponseError, c.expireat, key, 'bla')
        t = int(time.time() + 3)
        eq(await c.expireat(key, t), False)
        eq(await c.set(key, 1), True)
        t = int(time.time() + 10)
        eq(await c.expireat(key, t), True)
        ttl = await c.ttl(key)
        self.assertTrue(ttl > 0 and ttl <= 10)
        eq(await c.persist(key), True)
        eq(await c.ttl(key), -1)
        eq(await c.persist(key), False)

    async def test_keys(self):
        key = self.randomkey()
        keya = '%s_a' % key
        keyb = '%s_a' % key
        keyc = '%sc' % key
        c = self.client
        eq = self.assertEqual
        keys_with_underscores = set([keya.encode('utf-8'),
                                     keyb.encode('utf-8')])
        keys = keys_with_underscores.union(set([keyc.encode('utf-8')]))
        eq(await c.mset(keya, 1, keyb, 2, keyc, 3), True)
        k1 = await c.keys('%s_*' % key)
        k2 = await c.keys('%s*' % key)
        self.assertEqual(set(k1), keys_with_underscores)
        self.assertEqual(set(k2), keys)

    async def test_move(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        db = 3 if c.store.database == 4 else 4
        await self.wait.assertRaises(ResponseError, c.move, key, 'bla')
        eq(await c.move(key, db), False)
        eq(await c.set(key, 'ciao'), True)
        eq(await c.move(key, db), True)
        s2 = self.create_store(self.store.dns, database=db)
        c2 = s2.client()
        eq(await c2.get(key), b'ciao')
        eq(await c.exists(key), False)
        eq(await c.set(key, 'foo'), True)
        eq(await c.move(key, db), False)
        eq(await c.exists(key), True)

    async def __test_randomkey(self):
        # TODO: this test fails sometimes
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.mset(key, 1, key+'a', 2, key+'b', 3), True)
        key = await c.randomkey()
        eq(await c.exists(key), True)

    async def test_rename_renamenx(self):
        key = self.randomkey()
        des = self.randomkey()
        c = self.client
        eq = self.assertEqual
        await self.wait.assertRaises(ResponseError, c.rename, key, des)
        eq(await c.set(key, 'hello'), True)
        eq(await c.rename(key, des), True)
        eq(await c.exists(key), False)
        eq(await c.get(des), b'hello')
        eq(await c.set(key, 'ciao'), True)
        eq(await c.renamenx(key, des), False)
        eq(await c.renamenx(key, des+'a'), True)
        eq(await c.exists(key), False)

    ###########################################################################
    #    BAD REQUESTS
    # async def test_no_command(self):
    #     await self.wait.assertRaises(ResponseError, self.store.execute)

    # async def test_bad_command(self):
    #     await self.wait.assertRaises(ResponseError, self.store.execute,
    #                                   'foo')
    ###########################################################################
    #    STRINGS
    async def test_append(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.append(key, 'a1'), 2)
        eq(await c.get(key), b'a1')
        eq(await c.append(key, 'a2'), 4)
        eq(await c.get(key), b'a1a2')
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.append, key, 'g')

    async def test_bitcount(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.bitcount(key), 0)
        eq(await c.setbit(key, 5, 1), 0)
        eq(await c.bitcount(key), 1)
        eq(await c.setbit(key, 6, 1), 0)
        eq(await c.bitcount(key), 2)
        eq(await c.setbit(key, 5, 0), 1)
        eq(await c.bitcount(key), 1)
        eq(await c.setbit(key, 9, 1), 0)
        eq(await c.setbit(key, 17, 1), 0)
        eq(await c.setbit(key, 25, 1), 0)
        eq(await c.setbit(key, 33, 1), 0)
        eq(await c.bitcount(key), 5)
        eq(await c.bitcount(key, 0, -1), 5)
        eq(await c.bitcount(key, 2, 3), 2)
        eq(await c.bitcount(key, 2, -1), 3)
        eq(await c.bitcount(key, -2, -1), 2)
        eq(await c.bitcount(key, 1, 1), 1)
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.bitcount, key)

    async def test_bitop_not_empty_string(self):
        key = self.randomkey()
        des = key + 'd'
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, ''), True)
        eq(await c.bitop('not', des, key), 0)
        eq(await c.get(des), None)

    async def test_bitop_not(self):
        key = self.randomkey()
        des = key + 'd'
        c = self.client
        eq = self.assertEqual
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        eq(await c.set(key, test_str), True)
        eq(await c.bitop('not', des, key), 4)
        result = await c.get(des)
        self.assertEqual(int(binascii.hexlify(result), 16), correct)

    async def test_bitop_not_in_place(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        test_str = b'\xAA\x00\xFF\x55'
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        eq(await c.set(key, test_str), True)
        eq(await c.bitop('not', key, key), 4)
        result = await c.get(key)
        assert int(binascii.hexlify(result), 16) == correct

    async def test_bitop_single_string(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        test_str = b'\x01\x02\xFF'
        eq(await c.set(key, test_str), True)
        eq(await c.bitop('and', key+'1', key), 3)
        eq(await c.bitop('or', key+'2', key), 3)
        eq(await c.bitop('xor', key+'3', key), 3)
        eq(await c.get(key + '1'), test_str)
        eq(await c.get(key + '2'), test_str)
        eq(await c.get(key + '3'), test_str)

    async def test_bitop_string_operands(self):
        c = self.client
        eq = self.assertEqual
        key1 = self.randomkey()
        key2 = key1 + '2'
        des1 = key1 + 'd1'
        des2 = key1 + 'd2'
        des3 = key1 + 'd3'
        eq(await c.set(key1, b'\x01\x02\xFF\xFF'), True)
        eq(await c.set(key2, b'\x01\x02\xFF'), True)
        eq(await c.bitop('and', des1, key1, key2), 4)
        eq(await c.bitop('or', des2, key1, key2), 4)
        eq(await c.bitop('xor', des3, key1, key2), 4)
        res1 = await c.get(des1)
        res2 = await c.get(des2)
        res3 = await c.get(des3)
        self.assertEqual(int(binascii.hexlify(res1), 16), 0x0102FF00)
        self.assertEqual(int(binascii.hexlify(res2), 16), 0x0102FFFF)
        self.assertEqual(int(binascii.hexlify(res3), 16), 0x000000FF)

    async def test_decr(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.decr(key), -1)
        eq(await c.get(key), b'-1')
        eq(await c.decr(key), -2)
        eq(await c.get(key), b'-2')
        eq(await c.decr(key, 5), -7)
        eq(await c.get(key), b'-7')

    async def test_getbit(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.getbit(key, 5), 0)
        eq(await c.setbit(key, 5, 1), 0)
        eq(await c.getbit(key, 5), 1)
        await self.wait.assertRaises(ResponseError, c.getbit, key, -1)
        eq(await c.getbit(key, 4), 0)
        eq(await c.setbit(key, 4, 1), 0)
        # set bit 4
        eq(await c.getbit(key, 4), 1)
        eq(await c.getbit(key, 5), 1)
        # set bit 5 again
        eq(await c.setbit(key, 5, 1), 1)
        eq(await c.getbit(key, 5), 1)
        #
        eq(await c.getbit(key, 30), 0)
        #
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.getbit, key, 1)

    async def test_getrange(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.getrange(key, 0, 0), b'')
        eq(await c.set(key, 'Hello there'), True)
        eq(await c.getrange(key, 0, 0), b'H')
        eq(await c.getrange(key, 0, 4), b'Hello')
        eq(await c.getrange(key, 5, 5), b' ')
        eq(await c.getrange(key, 20, 25), b'')
        eq(await c.getrange(key, -5, -1), b'there')
        await self.wait.assertRaises(ResponseError, c.getrange, key, 1, 'b')
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.getrange, key, 1, 2)

    async def test_getset(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.getset(key, 'foo'), None)
        eq(await c.getset(key, 'bar'), b'foo')
        eq(await c.get(key), b'bar')

    async def test_get(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        await c.set(key, 'foo')
        eq(await c.get(key), b'foo')
        eq(await c.get('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'), None)

    async def test_incr(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.incr(key), 1)
        eq(await c.get(key), b'1')
        eq(await c.incr(key), 2)
        eq(await c.get(key), b'2')
        eq(await c.incr(key, 5), 7)
        eq(await c.get(key), b'7')

    async def test_incrby(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.incrby(key), 1)
        eq(await c.incrby(key, 4), 5)
        eq(await c.get(key), b'5')

    async def test_incrbyfloat(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.incrbyfloat(key), 1.0)
        eq(await c.get(key), b'1')
        eq(await c.incrbyfloat(key, 1.1), 2.1)
        eq(await c.get(key), b'2.1')

    async def test_mget(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        key3 = key2 + 'y'
        eq = self.assertEqual
        c = self.client
        eq(await c.set(key1, 'foox'), True)
        eq(await c.set(key2, 'fooxx'), True)
        eq(await c.mget(key1, key2, key3), [b'foox', b'fooxx', None])

    async def test_msetnx(self):
        key1 = self.randomkey()
        key2 = self.randomkey()
        key3 = self.randomkey()
        key4 = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.msetnx(key1, '1', key2, '2', key3, '3'), True)
        eq(await c.msetnx(key1, 'x', key4, 'y'), False)
        values = await c.mget(key1, key2, key3, key4)
        self.assertEqual(len(values), 4)
        for value, target in zip(values, (b'1', b'2', b'3', None)):
            self.assertEqual(value, target)

    async def test_set_ex(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, '1', ex=5), True)
        ttl = await c.ttl(key)
        self.assertTrue(0 < ttl <= 5)

    async def test_set_ex_timedelta(self):
        expire_at = datetime.timedelta(seconds=5)
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, '1', ex=expire_at), True)
        ttl = await c.ttl(key)
        self.assertTrue(0 < ttl <= 5)

    async def test_set_nx(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, '1', nx=True), True)
        eq(await c.set(key, '2', nx=True), False)
        eq(await c.get(key), b'1')

    async def test_set_px(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, '1', px=10000, nx=True), True)
        eq(await c.get(key), b'1')
        pttl = await c.pttl(key)
        self.assertTrue(0 < pttl <= 10000)
        ttl = await c.ttl(key)
        self.assertTrue(0 < ttl <= 10)

    async def test_set_px_timedelta(self):
        expire_at = datetime.timedelta(milliseconds=5000)
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, '1', px=expire_at, nx=True), True)
        eq(await c.get(key), b'1')
        pttl = await c.pttl(key)
        self.assertTrue(0 < pttl <= 5000)
        ttl = await c.ttl(key)
        self.assertTrue(0 < ttl <= 5)

    async def test_set_xx(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.set(key, '1', xx=True), False)
        eq(await c.get(key), None)
        eq(await c.set(key, 'bar'), True)
        eq(await c.set(key, '2', xx=True), True)
        eq(await c.get(key), b'2')

    async def test_setrange(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.setrange(key, 5, 'foo'), 8)
        eq(await c.get(key), b'\0\0\0\0\0foo')
        eq(await c.set(key, 'abcdefghijh'), True)
        eq(await c.setrange(key, 6, '12345'), 11)
        eq(await c.get(key), b'abcdef12345')

    async def test_strlen(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.strlen(key), 0)
        eq(await c.set(key, 'foo'), True)
        eq(await c.strlen(key), 3)

    ###########################################################################
    #    HASHES
    async def test_hdel(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.hdel(key, 'f1', 'f2', 'gh'), 0)
        eq(await c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}), True)
        eq(await c.hdel(key, 'f1', 'f2', 'gh'), 2)
        eq(await c.hdel(key, 'fgf'), 0)
        eq(await c.type(key), 'hash')
        eq(await c.hdel(key, 'f3'), 1)
        eq(await c.type(key), 'none')
        await self._remove_and_push(key, 0)
        await self.wait.assertRaises(ResponseError, c.hdel, key, 'foo')

    async def test_hexists(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.hexists(key, 'foo'), False)
        eq(await c.hmset(key, {'f1': 1, 'f2': 'hello', 'f3': 'foo'}), True)
        eq(await c.hexists(key, 'f3'), True)
        eq(await c.hexists(key, 'f5'), False)
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.hexists, key, 'foo')

    async def test_hset_hget(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.hget(key, 'foo'), None)
        eq(await c.hset(key, 'foo', 4), 1)
        eq(await c.hget(key, 'foo'), b'4')
        eq(await c.hset(key, 'foo', 6), 0)
        eq(await c.hget(key, 'foo'), b'6')
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.hset, key, 'foo', 7)
        await self.wait.assertRaises(ResponseError, c.hget, key, 'foo')
        await self.wait.assertRaises(ResponseError, c.hmset, key, 'foo')

    async def test_hgetall(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        h = {b'f1': b'1', b'f2': b'hello', b'f3': b'foo'}
        eq(await c.hgetall(key), {})
        eq(await c.hmset(key, h), True)
        eq(await c.hgetall(key), h)
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.hgetall, key)

    async def test_hincrby(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.hincrby(key, 'foo', 1), 1)
        eq(await c.hincrby(key, 'foo', 2), 3)
        eq(await c.hincrby(key, 'foo', -1), 2)
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.hincrby, key, 'foo', 3)

    async def test_hincrbyfloat(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.hincrbyfloat(key, 'foo', 1), 1.0)
        eq(await c.hincrbyfloat(key, 'foo', 2.5), 3.5)
        eq(await c.hincrbyfloat(key, 'foo', -1.1), 2.4)
        await self._remove_and_push(key)

    async def test_hkeys_hvals_hlen_hmget(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        h = {b'f1': b'1', b'f2': b'hello', b'f3': b'foo'}
        eq(await c.hkeys(key), [])
        eq(await c.hvals(key), [])
        eq(await c.hlen(key), 0)
        eq(await c.hmset(key, h), True)
        keys = await c.hkeys(key)
        vals = await c.hvals(key)
        self.assertEqual(sorted(keys), sorted(h))
        self.assertEqual(sorted(vals), sorted(h.values()))
        eq(await c.hlen(key), 3)
        eq(await c.hmget(key, 'f1', 'f3', 'hj'),
           {'f1': b'1', 'f3': b'foo', 'hj': None})
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.hkeys, key)
        await self.wait.assertRaises(ResponseError, c.hvals, key)
        await self.wait.assertRaises(ResponseError, c.hlen, key)
        await self.wait.assertRaises(ResponseError, c.hmget, key, 'f1', 'f2')

    async def test_hsetnx(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.hsetnx(key, 'a', 'foo'), 1)
        eq(await c.hget(key, 'a'), b'foo')
        eq(await c.hsetnx(key, 'a', 'bla'), 0)
        eq(await c.hget(key, 'a'), b'foo')
        await self._remove_and_push(key)
        await self.wait.assertRaises(ResponseError, c.hsetnx, key, 'a', 'jk')

    ###########################################################################
    #    LISTS
    async def test_blpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.assertEqual
        c = self.client
        await self.wait.assertRaises(ResponseError, c.blpop, key1, 'bla')
        eq(await c.rpush(key1, 1, 2), 2)
        eq(await c.rpush(key2, 3, 4), 2)
        eq(await c.blpop((key2, key1), 1), (bk2, b'3'))
        eq(await c.blpop((key2, key1), 1), (bk2, b'4'))
        eq(await c.blpop((key2, key1), 1), (bk1, b'1'))
        eq(await c.blpop((key2, key1), 1), (bk1, b'2'))
        eq(await c.blpop((key2, key1), 1), None)
        eq(await c.rpush(key1, '1'), 1)
        eq(await c.blpop(key1, 1), (bk1, b'1'))

    async def test_brpop(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        bk1 = key1.encode('utf-8')
        bk2 = key2.encode('utf-8')
        eq = self.assertEqual
        c = self.client
        eq(await c.rpush(key1, 1, 2), 2)
        eq(await c.rpush(key2, 3, 4), 2)
        eq(await c.brpop((key2, key1), 1), (bk2, b'4'))
        eq(await c.brpop((key2, key1), 1), (bk2, b'3'))
        eq(await c.brpop((key2, key1), 1), (bk1, b'2'))
        eq(await c.brpop((key2, key1), 1), (bk1, b'1'))
        eq(await c.brpop((key2, key1), 1), None)
        eq(await c.rpush(key1, '1'), 1)
        eq(await c.brpop(key1, 1), (bk1, b'1'))

    async def test_brpoplpush(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        eq = self.assertEqual
        c = self.client
        eq(await c.rpush(key1, 1, 2), 2)
        eq(await c.rpush(key2, 3, 4), 2)
        eq(await c.brpoplpush(key1, key2), b'2')
        eq(await c.brpoplpush(key1, key2), b'1')
        eq(await c.brpoplpush(key1, key2, timeout=1), None)
        eq(await c.lrange(key1, 0, -1), [])
        eq(await c.lrange(key2, 0, -1), [b'1', b'2', b'3', b'4'])

    async def test_brpoplpush_empty_string(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        eq = self.assertEqual
        c = self.client
        eq(await c.rpush(key1, ''), 1)
        eq(await c.brpoplpush(key1, key2), b'')

    async def test_lindex_llen(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.lindex(key, '0'), None)
        eq(await c.llen(key), 0)
        eq(await c.rpush(key, '1', '2', '3'), 3)
        eq(await c.lindex(key, '0'), b'1')
        eq(await c.lindex(key, '1'), b'2')
        eq(await c.lindex(key, '2'), b'3')
        eq(await c.lindex(key, '3'), None)
        eq(await c.llen(key), 3)
        await self._remove_and_sadd(key)
        await self.wait.assertRaises(ResponseError, c.lindex, key, '1')
        await self.wait.assertRaises(ResponseError, c.llen, key)

    async def test_linsert(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.linsert(key, 'after', '2', '2.5'), 0)
        eq(await c.rpush(key, '1', '2', '3'), 3)
        eq(await c.linsert(key, 'after', '2', '2.5'), 4)
        eq(await c.lrange(key, 0, -1), [b'1', b'2', b'2.5', b'3'])
        eq(await c.linsert(key, 'before', '2', '1.5'), 5)
        eq(await c.lrange(key, 0, -1), [b'1', b'1.5', b'2', b'2.5', b'3'])
        eq(await c.linsert(key, 'before', '100', '1.5'), -1)
        await self.wait.assertRaises(ResponseError, c.linsert, key,
                                     'banana', '2', '2.5')
        await self._remove_and_sadd(key)
        await self.wait.assertRaises(ResponseError, c.linsert, key,
                                     'after', '2', '2.5')

    async def test_lpop(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.lpop(key), None)
        eq(await c.rpush(key, 1, 2), 2)
        eq(await c.lpop(key), b'1')
        eq(await c.lpop(key), b'2')
        eq(await c.lpop(key), None)
        eq(await c.type(key), 'none')
        await self._remove_and_sadd(key, 0)
        await self.wait.assertRaises(ResponseError, c.lpop, key)
        await self.wait.assertRaises(ResponseError, c.lpush, key, 4)

    async def test_lset(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.rpush(key, '1', '2', '3'), 3)
        eq(await c.lrange(key, 0, -1), [b'1', b'2', b'3'])
        eq(await c.lset(key, 1, '4'), True)
        eq(await c.lrange(key, 0, 2), [b'1', b'4', b'3'])

    async def test_ltrim(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        eq(await c.rpush(key, '1', '2', '3'), 3)
        eq(await c.ltrim(key, 0, 1), True)
        eq(await c.lrange(key, 0, -1), [b'1', b'2'])

    async def test_lpushx_rpushx(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.lpushx(key, 'b'), 0)
        eq(await c.rpushx(key, 'b'), 0)
        eq(await c.lrange(key, 0, -1), [])
        eq(await c.lpush(key, 'a'), 1)
        eq(await c.lpushx(key, 'b'), 2)
        eq(await c.rpushx(key, 'c'), 3)
        eq(await c.lrange(key, 0, -1), [b'b', b'a', b'c'])
        await self._remove_and_sadd(key)
        await self.wait.assertRaises(ResponseError, c.lpushx, key, 'g')

    async def test_lrem(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.lrem(key, 1, 'a'), 0)
        eq(await c.rpush(key, 'a', 'a', 'a', 'b', 'a', 'a'), 6)
        eq(await c.lrem(key, 1, 'a'), 1)
        eq(await c.lrange(key, 0, -1), [b'a', b'a', b'b', b'a', b'a'])
        eq(await c.lrem(key, -1, 'a'), 1)
        eq(await c.lrange(key, 0, -1), [b'a', b'a', b'b', b'a'])
        eq(await c.lrem(key, 0, 'a'), 3)
        await self.wait.assertRaises(ResponseError, c.lrem, key, 'g', 'foo')
        eq(await c.lrange(key, 0, -1), [b'b'])
        eq(await c.lrem(key, 0, 'b'), 1)
        await self._remove_and_sadd(key, 0)
        await self.wait.assertRaises(ResponseError, c.lrem, key, 1)

    async def test_rpop(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.rpop(key), None)
        eq(await c.rpush(key, 1, 2), 2)
        eq(await c.rpop(key), b'2')
        eq(await c.rpop(key), b'1')
        eq(await c.rpop(key), None)
        eq(await c.type(key), 'none')

    async def test_rpoplpush(self):
        key1 = self.randomkey()
        key2 = key1 + 'x'
        eq = self.assertEqual
        c = self.client
        eq(await c.rpush(key1, 'a1', 'a2', 'a3'), 3)
        eq(await c.rpush(key2, 'b1', 'b2', 'b3'), 3)
        eq(await c.rpoplpush(key1, key2), b'a3')
        eq(await c.lrange(key1, 0, -1), [b'a1', b'a2'])
        eq(await c.lrange(key2, 0, -1), [b'a3', b'b1', b'b2', b'b3'])

    ###########################################################################
    #    SORT
    async def test_sort_basic(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        await c.rpush(key, '3', '2', '1', '4')
        eq(await c.sort(key), [b'1', b'2', b'3', b'4'])

    async def test_sort_limited(self):
        key = self.randomkey()
        c = self.client
        eq = self.assertEqual
        await c.rpush(key, '3', '2', '1', '4')
        eq(await c.sort(key, start=1, num=2), [b'2', b'3'])

    async def test_sort_by(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 8,
                  '%s:2' % key, 3,
                  '%s:3' % key, 5)
        pipe.rpush(key2, '3', '2', '1')
        res = await pipe.commit()
        self.assertEqual(len(res), 2)
        eq(await c.sort(key2, by='%s:*' % key), [b'2', b'3', b'1'])

    async def test_sort_get(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 'u1',
                  '%s:2' % key, 'u2',
                  '%s:3' % key, 'u3')
        pipe.rpush(key2, '3', '2', '1')
        res = await pipe.commit()
        self.assertEqual(len(res), 2)
        eq(await c.sort(key2, get='%s:*' % key), [b'u1', b'u2', b'u3'])

    async def test_sort_get_multi(self):
        key = self.randomkey()
        key2 = self.randomkey()
        c = self.client
        eq = self.assertEqual
        pipe = c.pipeline()
        pipe.mset('%s:1' % key, 'u1',
                  '%s:2' % key, 'u2',
                  '%s:3' % key, 'u3')
        pipe.rpush(key2, '3', '2', '1')
        res = await pipe.commit()
        self.assertEqual(len(res), 2)
        eq(await c.sort(key2, get=('%s:*' % key, '#')),
           [b'u1', b'1', b'u2', b'2', b'u3', b'3'])
        eq(await c.sort(key2, get=('%s:*' % key, '#'), groups=True),
           [(b'u1', b'1'), (b'u2', b'2'), (b'u3', b'3')])

    ###########################################################################
    #    SETS
    async def test_sadd_scard(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        members = (b'1', b'2', b'3', b'2')
        eq(await c.sadd(key, *members), 3)
        eq(await c.smembers(key), set(members))
        eq(await c.scard(key), 3)

    async def test_sdiff(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sdiff(key, key2), set((b'1', b'2', b'3')))
        eq(await c.sadd(key2, 2, 3), 2)
        eq(await c.sdiff(key, key2), set([b'1']))

    async def test_sdiffstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sdiffstore(des, key, key2), 3)
        eq(await c.smembers(des), set([b'1', b'2', b'3']))
        eq(await c.sadd(key2, 2, 3), 2)
        eq(await c.sdiffstore(des, key, key2), 1)
        eq(await c.smembers(des), set([b'1']))

    async def test_sinter(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sinter(key, key2), set())
        eq(await c.sadd(key2, 2, 3), 2)
        eq(await c.sinter(key, key2), set([b'2', b'3']))

    async def test_sinterstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sinterstore(des, key, key2), 0)
        eq(await c.smembers(des), set())
        eq(await c.sadd(key2, 2, 3), 2)
        eq(await c.sinterstore(des, key, key2), 2)
        eq(await c.smembers(des), set([b'2', b'3']))

    async def test_sismember(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sismember(key, 1), True)
        eq(await c.sismember(key, 2), True)
        eq(await c.sismember(key, 3), True)
        eq(await c.sismember(key, 4), False)

    async def test_smove(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.assertEqual
        c = self.client
        eq(await c.smove(key, key2, 1), False)
        eq(await c.sadd(key, 1, 2), 2)
        eq(await c.sadd(key2, 3, 4), 2)
        eq(await c.smove(key, key2, 1), True)
        eq(await c.smembers(key), set([b'2']))
        eq(await c.smembers(key2), set([b'1', b'3', b'4']))

    async def test_spop(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        value = await c.spop(key)
        self.assertTrue(value in set([b'1', b'2', b'3']))
        eq(await c.smembers(key), set([b'1', b'2', b'3']) - set([value]))

    async def test_srandmember(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        value = await c.srandmember(key)
        self.assertTrue(value in set((b'1', b'2', b'3')))
        eq(await c.smembers(key), set((b'1', b'2', b'3')))

    async def test_srandmember_multi_value(self):
        s = [b'1', b'2', b'3']
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, *s), 3)
        randoms = await c.srandmember(key, 2)
        self.assertEqual(len(randoms), 2)
        self.assertEqual(set(randoms).intersection(s), set(randoms))

    async def test_srem(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3, 4), 4)
        eq(await c.srem(key, 5), 0)
        eq(await c.srem(key, 5), 0)
        eq(await c.srem(key, 2, 4), 2)
        eq(await c.smembers(key), set([b'1', b'3']))

    async def test_sunion(self):
        key = self.randomkey()
        key2 = key + '2'
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sunion(key, key2), set((b'1', b'2', b'3')))
        eq(await c.sadd(key2, 2, 3, 4), 3)
        eq(await c.sunion(key, key2), set((b'1', b'2', b'3', b'4')))

    async def test_sunionstore(self):
        key = self.randomkey()
        key2 = key + '2'
        des = key + 'd'
        eq = self.assertEqual
        c = self.client
        eq(await c.sadd(key, 1, 2, 3), 3)
        eq(await c.sunionstore(des, key, key2), 3)
        eq(await c.smembers(des), set([b'1', b'2', b'3']))
        eq(await c.sadd(key2, 2, 3, 4), 3)
        eq(await c.sunionstore(des, key, key2), 4)
        eq(await c.smembers(des), set([b'1', b'2', b'3', b'4']))

    ###########################################################################
    #    SORTED SETS
    async def test_zadd_zcard(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3), 3)
        eq(await c.zrange(key, 0, -1), [b'a1', b'a2', b'a3'])
        eq(await c.zcard(key), 3)

    async def test_zcount(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3), 3)
        eq(await c.zcount(key, '-inf', '+inf'), 3)
        eq(await c.zcount(key, '(1', 2), 1)
        eq(await c.zcount(key, '(1', '(3'), 1)
        eq(await c.zcount(key, 1, 3), 3)

    async def test_zincrby(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3), 3)
        eq(await c.zincrby(key, 1, 'a2'), 3.0)
        eq(await c.zincrby(key, 5, 'a3'), 8.0)
        eq(await c.zscore(key, 'a2'), 3.0)
        eq(await c.zscore(key, 'a3'), 8.0)
        eq(await c.zscore(key, 'blaaa'), None)

    async def test_zinterstore_sum(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key1, a1=1, a2=2, a3=1), 3)
        eq(await c.zadd(key2, a1=2, a2=2, a3=2), 3)
        eq(await c.zadd(key3, a1=6, a3=5, a4=4), 3)
        eq(await c.zinterstore(des, (key1, key2, key3)), 2)
        eq(await c.zrange(des, 0, -1, withscores=True),
           Zset(((8.0, b'a3'), (9.0, b'a1'))))

    async def test_zinterstore_max(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key1, a1=1, a2=2, a3=1), 3)
        eq(await c.zadd(key2, a1=2, a2=2, a3=2), 3)
        eq(await c.zadd(key3, a1=6, a3=5, a4=4), 3)
        eq(await c.zinterstore(des, (key1, key2, key3), aggregate='max'), 2)
        eq(await c.zrange(des, 0, -1, withscores=True),
           Zset(((5.0, b'a3'), (6.0, b'a1'))))

    async def test_zinterstore_min(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key1, a1=1, a2=2, a3=1), 3)
        eq(await c.zadd(key2, a1=2, a2=2, a3=2), 3)
        eq(await c.zadd(key3, a1=6, a3=5, a4=4), 3)
        eq(await c.zinterstore(des, (key1, key2, key3), aggregate='min'), 2)
        eq(await c.zrange(des, 0, -1, withscores=True),
           Zset(((1.0, b'a3'), (1.0, b'a1'))))

    async def test_zinterstore_with_weights(self):
        des = self.randomkey()
        key1 = des + '1'
        key2 = des + '2'
        key3 = des + '3'
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key1, a1=1, a2=2, a3=1), 3)
        eq(await c.zadd(key2, a1=2, a2=2, a3=2), 3)
        eq(await c.zadd(key3, a1=6, a3=5, a4=4), 3)
        eq(await c.zinterstore(des, (key1, key2, key3), weights=(1, 2, 3)), 2)
        eq(await c.zrange(des, 0, -1, withscores=True),
           Zset(((20.0, b'a3'), (23.0, b'a1'))))

    async def test_zrange(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3), 3)
        eq(await c.zrange(key, 0, 1), [b'a1', b'a2'])
        eq(await c.zrange(key, 1, 2), [b'a2', b'a3'])
        eq(await c.zrange(key, 0, 1, withscores=True),
           Zset([(1, b'a1'), (2, b'a2')]))
        eq(await c.zrange(key, 1, 2, withscores=True),
           Zset([(2, b'a2'), (3, b'a3')]))

    async def test_zrangebyscore(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        eq(await c.zrangebyscore(key, 2, 4), [b'a2', b'a3', b'a4'])
        # slicing with start/num
        eq(await c.zrangebyscore(key, 2, 4, offset=1, count=2), [b'a3', b'a4'])
        # withscores
        eq(await c.zrangebyscore(key, 2, 4, withscores=True),
           Zset([(2.0, b'a2'), (3.0, b'a3'), (4.0, b'a4')]))

    async def test_zrank(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        eq(await c.zrank(key, 'a1'), 0)
        eq(await c.zrank(key, 'a2'), 1)
        eq(await c.zrank(key, 'a6'), None)

    async def test_zrem(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        eq(await c.zrem(key, 'a2'), 1)
        eq(await c.zrange(key, 0, -1), [b'a1', b'a3', b'a4', b'a5'])
        eq(await c.zrem(key, 'b'), 0)
        eq(await c.zrange(key, 0, -1), [b'a1', b'a3', b'a4', b'a5'])
        eq(await c.zrem(key, 'a3', 'a5', 'h'), 2)
        eq(await c.zrange(key, 0, -1), [b'a1', b'a4'])
        eq(await c.zrem(key, 'a1', 'a4'), 2)
        eq(await c.type(key), 'none')

    async def test_zremrangebyrank(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        eq(await c.zremrangebyrank(key, 1, 3), 3)
        eq(await c.zrange(key, 0, 5), [b'a1', b'a5'])

    async def test_zremrangebyscore(self):
        key = self.randomkey()
        eq = self.assertEqual
        c = self.client
        eq(await c.zadd(key, a1=1, a2=2, a3=3, a4=4, a5=5), 5)
        eq(await c.zremrangebyscore(key, 2, 4), 3)
        eq(await c.zrange(key, 0, -1), [b'a1', b'a5'])
        eq(await c.zremrangebyscore(key, 2, 4), 0)
        eq(await c.zrange(key, 0, -1), [b'a1', b'a5'])

    ###########################################################################
    #    CONNECTION
    async def test_ping(self):
        result = await self.client.ping()
        self.assertTrue(result)

    async def test_echo(self):
        result = await self.client.echo('Hello')
        self.assertEqual(result, b'Hello')

    ###########################################################################
    #    SERVER
    async def test_dbsize(self):
        await self.client.set('one_at_least', 'foo')
        result = await self.client.dbsize()
        self.assertTrue(result >= 1)

    async def test_info(self):
        info = await self.client.info()
        self.assertTrue(info)
        self.assertIsInstance(info, dict)

    async def test_time(self):
        t = await self.client.time()
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

    async def test_subscribe_one(self):
        key = self.randomkey()
        pubsub1 = self.client.pubsub()
        self.assertFalse(pubsub1._connection)
        # Subscribe to one channel
        await pubsub1.subscribe(key)
        count = await pubsub1.count(key)
        self.assertEqual(len(count), 1)
        self.assertEqual(count[key.encode('utf-8')], 1)
        #
        pubsub2 = self.client.pubsub()
        await pubsub2.subscribe(key)
        count = await pubsub1.count(key)
        self.assertEqual(len(count), 1)
        self.assertEqual(count[key.encode('utf-8')], 2)

    async def test_subscribe_many(self):
        base = self.randomkey()
        key1 = base + '_a'
        key2 = base + '_b'
        key3 = base + '_c'
        key4 = base + 'x'
        pubsub = self.client.pubsub()
        await pubsub.subscribe(key1, key2, key3, key4)
        channels = await pubsub.channels(base + '_*')
        self.assertEqual(len(channels), 3)
        count = await pubsub.count(key1, key2, key3)
        self.assertEqual(len(count), 3)
        self.assertEqual(count[key1.encode('utf-8')], 1)
        self.assertEqual(count[key2.encode('utf-8')], 1)
        self.assertEqual(count[key3.encode('utf-8')], 1)

    async def test_publish(self):
        pubsub = self.client.pubsub()
        listener = Listener()
        pubsub.add_client(listener)
        await pubsub.subscribe('chat')
        result = await pubsub.publish('chat', 'Hello')
        self.assertTrue(result >= 0)
        channel, message = await listener.get()
        self.assertEqual(channel, 'chat')
        self.assertEqual(message, b'Hello')

    async def test_publish_event(self):
        pubsub = self.client.pubsub(protocol=JsonProtocol())
        listener = Listener()
        pubsub.add_client(listener)
        await pubsub.subscribe('chat2')
        result = await pubsub.publish_event('chat2', 'room1', 'Hello')
        self.assertTrue(result >= 0)
        channel, message = await listener.get()
        self.assertEqual(channel, 'chat2')
        self.assertEqual(message['channel'], 'chat2')
        self.assertEqual(message['event'], 'room1')
        self.assertEqual(message['data'], 'Hello')

    async def test_pattern_subscribe(self):
        # switched off for redis. Issue #95
        if self.store.name == 'pulsar':
            eq = self.assertEqual
            pubsub = self.client.pubsub(protocol=StringProtocol())
            listener = Listener()
            pubsub.add_client(listener)
            eq(await pubsub.psubscribe('f*'), None)
            eq(await pubsub.publish('foo', 'hello foo'), 1)
            channel, message = await listener.get()
            self.assertEqual(channel, 'foo')
            self.assertEqual(message, 'hello foo')
            eq(await pubsub.punsubscribe(), None)
            # await listener.get()

    ###########################################################################
    #    TRANSACTION
    async def test_watch(self):
        key1 = self.randomkey()
        # key2 = key1 + '2'
        result = await self.client.watch(key1)
        self.assertEqual(result, 1)


class TestPulsarStore(RedisCommands, unittest.TestCase):
    app_cfg = None

    @classmethod
    async def setUpClass(cls):
        server = PulsarDS(name=cls.__name__.lower(),
                          bind='127.0.0.1:0',
                          concurrency=cls.cfg.concurrency,
                          redis_py_parser=cls.redis_py_parser)
        cls.app_cfg = await pulsar.send('arbiter', 'run', server)
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
