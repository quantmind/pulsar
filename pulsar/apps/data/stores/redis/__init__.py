'''
Pulsar is shipped with a :class:`.Store` implementation for redis_
and :ref:`pulsard-ds <pulsar-data-store>` servers.

.. _redis: http://redis.io/

Redis Store
~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.stores.redis.store.RedisStore
   :members:
   :member-order: bysource


Redis Client
~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.stores.redis.client.RedisClient
   :members:
   :member-order: bysource

Redis Pipeline
~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.stores.redis.client.Pipeline
   :members:
   :member-order: bysource
'''
from pulsar.utils.config import Global
from pulsar.apps.data import register_store

from .store import RedisStore
from .client import RedisScript


__all__ = ['RedisStore', 'RedisScript']


class RedisServer(Global):
    name = 'redis_server'
    flags = ['--redis-server']
    meta = "CONNECTION_STRING"
    default = '127.0.0.1:6379'
    desc = 'Default connection string for the redis server'


register_store('redis', 'pulsar.apps.data.stores.RedisStore')
