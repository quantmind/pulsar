from pulsar.utils.config import Global
from pulsar.apps.data import register_store

from ..pulsards import store


__all__ = ['RedisStore']


class RedisStore(store.PulsarStore):
    pass


class RedisServer(Global):
    name = 'redis_server'
    flags = ['--redis-server']
    meta = "CONNECTION_STRING"
    default = '127.0.0.1:6379'
    desc = 'Default connection string for the redis server'


register_store('redis',
               'pulsar.apps.data.stores.RedisStore')
