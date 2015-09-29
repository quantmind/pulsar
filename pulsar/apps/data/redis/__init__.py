'''
Pulsar is shipped with a :class:`.Store` implementation for redis_
and :ref:`pulsard-ds <pulsar-data-store>` servers.

.. _redis: http://redis.io/

Redis Store
~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.redis.store.RedisStore
   :members:
   :member-order: bysource


Redis Client
~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.redis.client.RedisClient
   :members:
   :member-order: bysource

Redis Pipeline
~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.redis.client.Pipeline
   :members:
   :member-order: bysource
'''
from pulsar.utils.config import Global
from pulsar.apps.data import register_store
from pulsar.apps.ds import RedisError, NoScriptError, redis_parser

from .store import RedisStore, RedisStoreConnection
from .client import ResponseError, Consumer, Pipeline
from .lock import RedisScript, LockError


__all__ = ['RedisStore', 'RedisError', 'NoScriptError', 'redis_parser',
           'RedisStoreConnection', 'Consumer', 'Pipeline', 'ResponseError',
           'RedisScript', 'LockError']


class RedisServer(Global):
    name = 'redis_server'
    flags = ['--redis-server']
    meta = "CONNECTION_STRING"
    default = '127.0.0.1:6379/7'
    desc = 'Default connection string for the redis server'


register_store('redis', 'pulsar.apps.data.RedisStore')
