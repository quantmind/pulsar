import uuid
from asyncio import sleep

from pulsar import isawaitable, LockError, LockBase


class RedisScript:
    '''An executable Lua script object
    '''
    def __init__(self, script):
        self.script = script
        self.sha = None

    async def __call__(self, client, keys=None, args=None):
        '''Execute the script, passing any required ``args``
        '''
        if self.sha not in client.store.loaded_scripts:
            sha = await client.immediate_execute('SCRIPT', 'LOAD', self.script)
            self.sha = sha.decode('utf-8')
            client.store.loaded_scripts.add(self.sha)

        result = client.evalsha(self.sha, keys, args)
        if isawaitable(result):
            result = await result
        return result


class Lock(LockBase):
    """Asynchronous locking primitive for distributing computing
    """
    def __init__(self, client, name, timeout=None, blocking=True, sleep=0.2):
        super().__init__(name, loop=client._loop, timeout=timeout,
                         blocking=blocking)
        self._token = None
        self.client = client
        self.sleep = sleep
        if self.blocking:
            self.sleep = min(self.sleep, self.blocking)

    def locked(self):
        ''''Return the token that acquire the lock or None.
        '''
        return bool(self._token)

    async def _acquire(self):
        token = uuid.uuid1().hex.encode('utf-8')
        timeout = self.timeout and int(self.timeout * 1000) or ''
        acquired = await self.lua_acquire(self.client, keys=[self.name],
                                          args=[token, timeout])
        if acquired:
            self._token = token

        return acquired

    async def acquire(self):
        loop = self._loop
        start = loop.time()
        acquired = await self._acquire()
        if self.blocking is False:
            return acquired
        timeout = self.blocking
        if timeout is True:
            timeout = 0
        while not acquired:
            if timeout and loop.time() - start >= timeout:
                break
            await sleep(self.sleep)
            acquired = await self._acquire()

        return acquired

    async def release(self):
        expected_token = self._token
        if not expected_token:
            raise LockError("Cannot release an unlocked lock")
        released = await self.lua_release(self.client, keys=[self.name],
                                          args=[expected_token])
        self._token = None
        if not released:
            raise LockError("Cannot release a lock that's no longer owned")
        return True

    lua_acquire = RedisScript("""
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then
            if ARGV[2] ~= '' then
                redis.call('pexpire', KEYS[1], ARGV[2])
            end
            return 1
        end
        return 0
    """)

    # KEYS[1] - lock name
    # ARGS[1] - token
    # return 1 if the lock was released, otherwise 0
    lua_release = RedisScript("""
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    """)
