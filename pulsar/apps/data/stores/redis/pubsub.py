from functools import partial

from pulsar import task, in_loop, Protocol, EventHandler, coroutine_return
from pulsar.apps import data


class PubsubProtocol(Protocol):

    def __init__(self, handler, *args, **kw):
        super(PubsubProtocol, self).__init__(*args, **kw)
        self.parser = self._producer._parser_class()
        self.handler = handler

    def execute(self, *args):
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


class PubSub(data.PubSub):
    '''Asynchronous Publish/Subscriber handler for pulsar and redis stores.
    '''
    def publish(self, channel, message):
        if self._protocol:
            message = self._protocol.encode(message)
        return self.store.execute('PUBLISH', channel, message)

    def count(self, *channels):
        kw = {'subcommand': 'numsub'}
        return self.store.execute('PUBSUB', 'NUMSUB', *channels, **kw)

    def channels(self, pattern=None):
        '''Lists the currently active channels matching ``pattern``
        '''
        if pattern:
            return self.store.execute('PUBSUB', 'CHANNELS', pattern)
        else:
            return self.store.execute('PUBSUB', 'CHANNELS')

    def psubscribe(self, pattern, *patterns):
        return self._subscribe('PSUBSCRIBE', pattern, *patterns)

    @in_loop
    def punsubscribe(self, *patterns):
        if self._connection:
            self._connection.execute('PUNSUBSCRIBE', *patterns)

    def subscribe(self, channel, *channels):
        return self._subscribe('SUBSCRIBE', channel, *channels)

    @in_loop
    def unsubscribe(self, *channels):
        '''Un-subscribe from a list of ``channels``.
        '''
        if self._connection:
            self._connection.execute('UNSUBSCRIBE', *channels)

    @in_loop
    def close(self):
        '''Stop listening for messages.
        '''
        if self._connection:
            self._connection.execute('PUNSUBSCRIBE')
            self._connection.execute('UNSUBSCRIBE')

    #    INTERNALS
    @task
    def _subscribe(self, *args):
        if not self._connection:
            protocol_factory = partial(PubsubProtocol, self,
                                       producer=self.store)
            self._connection = yield self.store.connect(protocol_factory)
            self._connection.execute(*args)
        coroutine_return()
