from functools import partial

from stdnet import getdb
from stdnet.lib.redis.async import RedisProtocol

from pulsar import async, Deferred
from pulsar.apps import pubsub
from pulsar.utils.log import local_property


class PubSubBackend(pubsub.PubSubBackend):
    '''Implements :class:`PubSub` using a redis backend.'''
    @local_property
    def redis(self):
        return getdb(self.connection_string, timeout=0).client
    
    @property
    def consumer(self):
        return self.local.consumer
    
    def publish(self, channel, message):
        return self.redis.publish(channel, message)
       
    @async()
    def subscribe(self, *channels):
        # Subscribe to redis. Don't release connection so that we can use the
        # publish command too.
        channels, patterns = self._channel_patterns(channels)        
        if channels:
            yield self._execute('subscribe', *channels)
        if patterns:
            yield self._execute('psubscribe', *patterns)
    
    @async()
    def unsubscribe(self, *channels):
        channels, patterns = self._channel_patterns(channels)
        if not channels and not patterns:
            yield self._execute('unsubscribe')
            yield self._execute('punsubscribe')
        else:
            if channels:
                yield self._execute('unsubscribe', *channels)
            if patterns:
                yield self._execute('punsubscribe', *patterns)
            
    @async()
    def close(self):
        result = yield super(PubSubBackend, self).close()
        if self.consumer:
            self.consumer.connection.close()
            self.local.consumer = None
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
    
    def _execute(self, command, *args):
        cbk = Deferred().add_callback(self._request_finished)
        return self.redis.execute_command(command, *args,
                                          consumer=self.consumer,
                                          on_finished=cbk)
        
    def _request_finished(self, consumer_response):
        consumer, response = consumer_response
        if self.local.consumer != consumer:
            self.local.consumer = consumer
            consumer.bind_event('on_message', self.on_message)
        return self.on_message(response)
    
    def on_message(self, response):
        command = response[0]
        if command == b'message':
            response = response[1:3]
            self.broadcast(*response)
        elif command == b'pmessage':
            response = response[2:4]
            self.broadcast(*response)
        elif command == b'subscribe' or command == b'psubscribe':
            response = response[2]
        elif command == b'unsubscribe' or command == b'punsubscribe':
            response = response[2]
        return response
            