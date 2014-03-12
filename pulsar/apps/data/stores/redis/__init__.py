from pulsar.apps.data import register_store

from ..pulsards import store


__all__ = ['RedisStore']


class RedisStore(store.PulsarStore):
    pass


register_store('redis',
               'pulsar.apps.data.stores.RedisStore')
