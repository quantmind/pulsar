import unittest
import asyncio

from pulsar import Lock


class TestLock(unittest.TestCase):

    def test_lock_init(self):
        lock = Lock('foo')
        self.assertEqual(lock.name, 'foo')
        lock2 = Lock('foo')
        self.assertEqual(lock2.name, 'foo')
        self.assertNotEqual(lock, lock2)
        self.assertEqual(lock._lock, lock2._lock)

    async def test_lock(self):
        lock1 = Lock('test', blocking=False)
        lock2 = Lock('test', blocking=False)
        self.assertEqual(await lock1.acquire(), True)
        self.assertEqual(await lock2.acquire(), False)
        self.assertFalse(lock2.locked())
        await lock1.release()
        self.assertFalse(lock1.locked())

    async def test_lock_blocking(self):
        lock1 = Lock('test1')
        lock2 = Lock('test1', blocking=1)
        self.assertEqual(await lock1.acquire(), True)
        start = lock2._loop.time()
        self.assertEqual(await lock2.acquire(), False)
        self.assertGreaterEqual(lock2._loop.time() - start, 1)
        self.assertFalse(lock2.locked())
        await lock1.release()
        self.assertFalse(lock1.locked())

    async def test_lock_timeout(self):
        lock = Lock('test2', timeout=1)
        self.assertEqual(await lock.acquire(), True)
        await asyncio.sleep(1.5)
        self.assertFalse(lock.locked())

    async def test_lock_timeout_lock(self):
        lock1 = Lock('test3', timeout=1)
        lock2 = Lock('test3', blocking=True)
        self.assertEqual(await lock1.acquire(), True)
        self.assertTrue(lock1.locked())
        future = asyncio.ensure_future(lock2.acquire())
        await asyncio.sleep(1.5)
        self.assertFalse(lock1.locked())
        await future
        self.assertTrue(lock2.locked())

    async def test_context(self):
        lock = Lock('test4', blocking=1)
        async with Lock('test4'):
            self.assertEqual(await lock.acquire(), False)
            self.assertFalse(lock.locked())
        async with lock:
            self.assertTrue(lock.locked())
        self.assertFalse(lock.locked())
