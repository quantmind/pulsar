from functools import partial

from pulsar import Protocol
from pulsar.apps.data import PubSub


class PubsubProtocol(Protocol):

    def __init__(self, handler, **kw):
        super().__init__(handler._loop, **kw)
        self.parser = self._producer._parser_class()
        self.handler = handler

    async def execute(self, *args):
        # must be an asynchronous object like the base class method
        chunk = self.parser.multi_bulk(args)
        self._transport.write(chunk)

    def data_received(self, data):
        parser = self.parser
        parser.feed(data)
        response = parser.get()
        while response is not False:
            if not isinstance(response, Exception):
                if isinstance(response, list):
                    command = response[0]
                    if command == b'message':
                        response = response[1:3]
                        self.handler.broadcast(response)
                    elif command == b'pmessage':
                        response = response[2:4]
                        self.handler.broadcast(response)
            else:
                raise response
            response = parser.get()


class RedisPubSub(PubSub):
    '''Asynchronous Publish/Subscriber handler for pulsar and redis stores.
    '''
    def publish(self, channel, message):
        if self._protocol:
            message = self._protocol.encode(message)
        return self.store.execute('PUBLISH', channel, message)

    def count(self, *channels):
        kw = {'subcommand': 'numsub'}
        return self.store.execute('PUBSUB', 'NUMSUB', *channels, **kw)

    def count_patterns(self):
        kw = {'subcommand': 'numpat'}
        return self.store.execute('PUBSUB', 'NUMPAT', **kw)

    def channels(self, pattern=None):
        '''Lists the currently active channels matching ``pattern``
        '''
        if pattern:
            return self.store.execute('PUBSUB', 'CHANNELS', pattern)
        else:
            return self.store.execute('PUBSUB', 'CHANNELS')

    def psubscribe(self, pattern, *patterns):
        return self._subscribe('PSUBSCRIBE', pattern, *patterns)

    def punsubscribe(self, *patterns):
        if self._connection:
            return self._connection.execute('PUNSUBSCRIBE', *patterns)

    def subscribe(self, channel, *channels):
        return self._subscribe('SUBSCRIBE', channel, *channels)

    def unsubscribe(self, *channels):
        '''Un-subscribe from a list of ``channels``.
        '''
        if self._connection:
            return self._connection.execute('UNSUBSCRIBE', *channels)

    async def close(self):
        '''Stop listening for messages.
        '''
        if self._connection:
            await self._connection.execute('PUNSUBSCRIBE')
            await self._connection.execute('UNSUBSCRIBE')

    #    INTERNALS
    async def _subscribe(self, *args):
        if not self._connection:
            protocol_factory = partial(PubsubProtocol, self,
                                       producer=self.store)
            self._connection = await self.store.connect(protocol_factory)
        result = await self._connection.execute(*args)
        return result
