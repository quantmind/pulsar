import asyncio

from pulsar import ensure_future
from pulsar.apps.data import LockError


class RedisLockTests:

    async def _release(self, lock, time):
        await asyncio.sleep(time)
        self.assertEqual(await lock.release(), True)

    async def test_lock(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock = self.client.lock(key)
        self.assertEqual(lock.name, key)
        self.assertFalse(lock._token)

        eq(await lock.acquire(), True)
        self.assertTrue(lock._token)
        eq(await self.client.get(key), lock._token)
        # assert sr.ttl('foo') == -1
        eq(await lock.release(), True)
        eq(await self.client.get(key), None)

    async def test_competing_locks(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock1 = self.client.lock(key, blocking=False)
        lock2 = self.client.lock(key, blocking=False)
        eq(await lock1.acquire(), True)
        eq(await lock2.acquire(), False)
        eq(await lock1.release(), True)
        eq(await lock2.acquire(), True)
        eq(await lock1.acquire(), False)
        eq(await lock2.release(), True)

    async def test_timeout(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock = self.client.lock(key, timeout=10)
        eq(await lock.acquire(), True)
        ttl = await self.client.ttl(lock.name)
        self.assertTrue(2 < ttl <= 10)
        eq(await lock.release(), True)

    async def test_float_timeout(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock = self.client.lock(key, timeout=9.5)
        eq(await lock.acquire(), True)
        ttl = await self.client.pttl(lock.name)
        self.assertTrue(4000 < ttl <= 9500)
        eq(await lock.release(), True)

    async def test_blocking_timeout(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock1 = self.client.lock(key)
        lock2 = self.client.lock(key, blocking=0.5)
        eq(await lock1.acquire(), True)
        start = lock1._loop.time()
        eq(await lock2.acquire(), False)
        self.assertTrue(lock1._loop.time() - start > 0.5)
        eq(await lock1.release(), True)

    async def test_blocking_timeout_acquire(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock1 = self.client.lock(key)
        lock2 = self.client.lock(key, blocking=5)
        eq(await lock1.acquire(), True)
        ensure_future(self._release(lock1, 0.5))
        start = lock2._loop.time()
        eq(await lock2.acquire(), True)
        self.assertTrue(5 > lock2._loop.time() - start > 0.5)
        eq(await lock2.release(), True)

    def test_high_sleep_min(self):
        lock = self.client.lock('foo', blocking=1, sleep=2)
        self.assertEqual(lock.sleep, 1)

    async def test_releasing_lock_no_longer_owned_raises_error(self):
        key = self.randomkey()
        eq = self.assertEqual
        lock = self.client.lock(key)
        eq(await lock.acquire(), True)
        # manually change the token
        await self.client.set(key, 'a')
        await self.wait.assertRaises(LockError, lock.release)
        # even though we errored, the token is still cleared
        self.assertEqual(lock._token, None)

    async def test_context_manager(self):
        key = self.randomkey()
        lock2 = self.client.lock(key, blocking=False)
        async with self.client.lock(key):
            a = await lock2.acquire()
            self.assertFalse(a)
        a = await lock2.acquire()
        self.assertTrue(a)
        await lock2.release()

    async def test_context_manager_error(self):
        key = self.randomkey()
        lock2 = self.client.lock(key, blocking=False)

        async def _lock():
            async with lock2:
                pass

        async with self.client.lock(key):
            await self.wait.assertRaises(LockError, _lock)
