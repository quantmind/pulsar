'''
Redis :class:`.Store`, requires redis-py_::

    store = create_store('redis://localhost:6739/11')
    redis = store.client()

The ``redis`` object in the example can be used exactly like the redis
client in redis-py_.


.. _redis-py: https://github.com/andymccurdy/redis-py
'''
from .client import HAS_C_EXTENSIONS, RedisParser, PyRedisParser
from . import store
