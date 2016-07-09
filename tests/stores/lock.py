import asyncio

from pulsar import ensure_future
from pulsar.apps.data import LockError


class RedisLockTests:

    async def _release(self, lock, time):
        await asyncio.sleep(time)
        await self.wait.assertEqual(lock.release(), True)

    @asyncio.coroutine
    def test_lock(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock = self.client.lock(key)
        self.assertEqual(lock.name, key)
        self.assertFalse(lock.token)

        yield from eq(lock.acquire(), True)
        self.assertTrue(lock.token)
        yield from eq(self.client.get(key), lock.token)
        # assert sr.ttl('foo') == -1
        yield from eq(lock.release(), True)
        yield from eq(self.client.get(key), None)

    async def test_competing_locks(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock1 = self.client.lock(key, blocking=None)
        lock2 = self.client.lock(key, blocking=None)
        await eq(lock1.acquire(), True)
        await eq(lock2.acquire(), False)
        await eq(lock1.release(), True)
        await eq(lock2.acquire(), True)
        await eq(lock1.acquire(), False)
        await eq(lock2.release(), True)

    @asyncio.coroutine
    def test_timeout(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock = self.client.lock(key, timeout=10)
        yield from eq(lock.acquire(), True)
        ttl = yield from self.client.ttl(lock.name)
        self.assertTrue(2 < ttl <= 10)
        yield from eq(lock.release(), True)

    @asyncio.coroutine
    def test_float_timeout(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock = self.client.lock(key, timeout=9.5)
        yield from eq(lock.acquire(), True)
        ttl = yield from self.client.pttl(lock.name)
        self.assertTrue(4000 < ttl <= 9500)
        yield from eq(lock.release(), True)

    @asyncio.coroutine
    def test_blocking_timeout(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock1 = self.client.lock(key)
        lock2 = self.client.lock(key, blocking=0.5)
        yield from eq(lock1.acquire(), True)
        start = lock1._loop.time()
        yield from eq(lock2.acquire(), False)
        self.assertTrue(lock1._loop.time() - start > 0.5)
        yield from eq(lock1.release(), True)

    @asyncio.coroutine
    def test_blocking_timeout_acquire(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock1 = self.client.lock(key)
        lock2 = self.client.lock(key, blocking=5)
        yield from eq(lock1.acquire(), True)
        ensure_future(self._release(lock1, 0.5))
        start = lock2._loop.time()
        yield from eq(lock2.acquire(), True)
        self.assertTrue(5 > lock2._loop.time() - start > 0.5)
        yield from eq(lock2.release(), True)

    def test_high_sleep_min(self):
        lock = self.client.lock('foo', blocking=1, sleep=2)
        self.assertEqual(lock.sleep, 1)

    @asyncio.coroutine
    def test_releasing_lock_no_longer_owned_raises_error(self):
        key = self.randomkey()
        eq = self.wait.assertEqual
        lock = self.client.lock(key)
        yield from eq(lock.acquire(), True)
        # manually change the token
        yield from self.client.set(key, 'a')
        yield from self.wait.assertRaises(LockError, lock.release)
        # even though we errored, the token is still cleared
        self.assertEqual(lock.token, None)

    async def test_context_manager(self):
        key = self.randomkey()
        lock2 = self.client.lock(key, blocking=None)
        async with self.client.lock(key):
            a = await lock2.acquire()
            self.assertFalse(a)
        a = await lock2.acquire()
        self.assertTrue(a)
        await lock2.release()

    async def test_context_manager_error(self):
        key = self.randomkey()
        lock2 = self.client.lock(key, blocking=None)

        async def _lock():
            async with lock2:
                pass

        async with self.client.lock(key):
            await self.wait.assertRaises(LockError, _lock)
