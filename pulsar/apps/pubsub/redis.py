from stdnet import getdb

from pulsar import async
from pulsar.apps import pubsub
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
        redis = getdb(self.connection_string, timeout=0).client.pubsub()
        redis.bind_event('on_message', self.on_message)
        return redis
    
    @async()
    def publish(self, channel, message):
        # make it asynchronous so that errors are logged
        return self.redis.publish(channel, message)
       
    def subscribe(self, *channels):
        return self.redis.subscribe(*channels)
    
    def unsubscribe(self, *channels):
        return self.redis.unsubscribe(*channels)
    
    def on_message(self, channel_message):
        self.broadcast(*channel_message)