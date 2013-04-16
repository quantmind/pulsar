from functools import partial

from stdnet import getdb
from stdnet.lib.redis.async import RedisProtocol

from pulsar import async
from pulsar.apps import pubsub


class PubSub(pubsub.PubSub):
    '''Implements :class:`PubSub` using a redis backend.'''
    
    def setup(self, **params):
        self.redis = getdb(self.connection_string, timeout=0).client
        self.consumer = None
        super(PubSub, self).setup(**params)
    
    def publish(self, channel, message):
        message = self.encode(message)
        return self.redis.publish(channel, message)
       
    @async()
    def subscribe(self, *channels):
        # Subscribe to redis. Don't release connection so that we can use the
        # publish command too.
        channels, patterns = self._channel_patterns(channels)        
        if channels:
            yield self.redis.execute_command('subscribe', *channels,
                                             on_finished=self._subscribe,
                                             consumer=self.consumer)
        if patterns:
            yield self.redis.execute_command('psubscribe', *patterns,
                                             on_finished=self._subscribe,
                                             consumer=self.consumer)
            
    def unsubscribe(self, *channels):
        channels, patterns = self._channel_patterns(channels)
        if not channels and not patterns:
            yield self.redis.execute_command('unsubscribe',
                                             consumer=self.consumer)
            yield self.redis.execute_command('punsubscribe',
                                             consumer=self.consumer)
        else:
            if channels:
                yield self.redis.execute_command('unsubscribe', *channels,
                                                 consumer=self.consumer)
            if patterns:
                yield self.redis.execute_command('punsubscribe', *patterns,
                                                 consumer=self.consumer)
            
    @async()
    def close(self):
        result = yield self.unsubscribe()
        if self.consumer:
            self.consumer.connection.close()
        yield result
     
    def _channel_patterns(self, channels):
        patterns = []
        simples = []
        for c in channels:
            if '*' in c:
                if c != '*':
                    patterns.append(c)
            else:
                simples.append(c)
        return simples, patterns
    
    def _subscribe(self, protocol, response):
        if not self.consumer:
            reader = protocol.current_request.reader
            connection = self.redis.connection_pool.upgrade(
                                            protocol.connection,
                                            partial(Subscriber, reader, self))
            self.consumer = connection.consumer_factory(connection)
        
        
class Subscriber(RedisProtocol):
    
    def __init__(self, reader, pubsub, connection=None):
        self.reader = reader
        self.pubsub = pubsub
        super(Subscriber, self).__init__(connection=connection)
        
    def data_received(self, data):
        self.reader.feed(data)
        response = self.reader.gets()
        if response is not False:
            self.on_response(response)
    
    def on_response(self, response):
        "Parse the response from a publish/subscribe command"
        command = response[0].decode('utf-8')
        channel = response[1]
        if command in ('message', 'pmessage'):
            self.pubsub.broadcast(channel, response[2])