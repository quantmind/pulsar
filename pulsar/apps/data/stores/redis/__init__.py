from ..pulsards import store, register_store


class RedisStore(store.PulsarStore):
    pass


register_store('redis',
               'pulsar.apps.data.stores.redis.RedisStore')
