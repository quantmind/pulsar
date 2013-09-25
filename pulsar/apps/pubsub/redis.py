from pulsar.apps import pubsub, redis
from pulsar.utils.log import local_property
from pulsar.utils.internet import get_connection_string


class PubSubBackend(pubsub.PubSubBackend):
    '''Implements :class:`PubSub` using a redis backend.'''

    @classmethod
    def get_connection_string(cls, scheme, address, params, name):
        if name:
            params['namespace'] = '%s.' % name
        return get_connection_string(scheme, address, params)

    @local_property
    def redis(self):
        self._client = redis.RedisPool()
        client = self._client.from_connection_string(self.connection_string)
        pubsub = client.pubsub()
        pubsub.bind_event('on_message', self.on_message)
        self.namespace = pubsub.extra.get('namespace')
        return pubsub

    def publish(self, channel, message):
        redis = self.redis
        if self.namespace:
            channel = '%s%s' % (self.namespace, channel)
        return redis.publish(channel, message)

    def subscribe(self, *channels):
        redis = self.redis
        if self.namespace:
            channels = tuple(('%s%s' % (self.namespace, c) for c in channels))
        return redis.subscribe(*channels)

    def unsubscribe(self, *channels):
        redis = self.redis
        if self.namespace:
            channels = tuple(('%s%s' % (self.namespace, c) for c in channels))
        return redis.unsubscribe(*channels)

    def on_message(self, channel_message):
        self.broadcast(*channel_message)
