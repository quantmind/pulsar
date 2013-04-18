from stdnet import getdb

from pulsar.apps import pubsub
from pulsar.utils.log import local_property


class PubSubBackend(pubsub.PubSubBackend):
    '''Implements :class:`PubSub` using a redis backend.'''
    @local_property
    def redis(self):
        redis = getdb(self.connection_string, timeout=0).client.pubsub()
        redis.bind_event('on_message', self.on_message)
        return redis
    
    def publish(self, channel, message):
        return self.redis.publish(channel, message)
       
    def subscribe(self, *channels):
        return self.redis.subscribe(*channels)
    
    def unsubscribe(self, *channels):
        return self.redis.unsubscribe(*channels)
    
    def on_message(self, channel_message):
        self.broadcast(*channel_message)