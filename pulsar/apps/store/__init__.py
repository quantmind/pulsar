'''A Key-value store interface
'''
from pulsar.utils.system import json

dumps = json.dumps


OK = dumps({'type': 0, 'value': 'OK'})
OK0 = dumps({'type': 0, 'value': 0})
OK1 = dumps({'type': 0, 'value': 1})


class Store(object):

    def __init_(self, loop):
        self._loop = loop
        self._store = {}
        self._timeouts = {}

    def get(self, key):
        value = self._store.get(key)
        return dumps({'type': 1,
                      'value': value})

    def set(self, key, value, timeout=None):
        if value:
            if key in self._timeouts:
                handle = self._timeouts.pop(key)
                handle.cancel()
            self._store[key] = value
            if timeout:
                self.expire(key, timeout)
            return OK

    def expire(self, key, timeout):
        if key and timeout:
            self._timeouts[key] = self._loop.call_later(
                timeout, self._expire, key)
            return OK1
        return OK0

    def zadd(self, *key_score_members):
        container = self._container
        for key, score, member in key_score_members:
            c = container(key, skiplist)
            c.insert(score, member)

    def sadd(self, *key_members):
        for key, member in key_members:
            c = container(key, skiplist)
            c.insert(score, member)

    ###    INTERNALS
    def _expire(self, key):
        self._timeouts.pop(key, None)
        self._store.pop(key, None)
