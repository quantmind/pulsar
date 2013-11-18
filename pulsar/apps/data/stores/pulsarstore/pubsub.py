from functools import partial

from pulsar import in_loop_thread, Protocol, EventHandler, coroutine_return

from . import base


class PubsubProtocol(Protocol):

    def __init__(self, handler, *args, **kw):
        super(PubsubProtocol, self).__init__(*args, **kw)
        self.parser = self._producer._parser_class()
        self.handler = handler

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
                        self.handler.fire_event('on_message', response)
                    elif command == b'pmessage':
                        response = response[2:4]
                        self.handler.fire_event('on_message', response)
            else:
                raise response
            response = parser.get()


class PubSub(base.PubSub):
    '''Asynchronous Publish/Subscriber handler for pulsar and redis stores.
    '''
    def publish(self, channel, message):
        return self.store.execute('PUBLISH', channel, message)

    def count(self, *channels):
        return self.store.execute('PUBSUB', 'NUMSUB', *channels)

    def channels(self, pattern=None):
        if pattern:
            return self.store.execute('PUBSUB', 'CHANNELS', pattern)
        else:
            return self.store.execute('PUBSUB', 'CHANNELS')

    @in_loop_thread
    def psubscribe(self, pattern, *patterns):
        return self._subscribe('PSUBSCRIBE', pattern, *patterns)

    @in_loop_thread
    def punsubscribe(self, *channels):
        if self._connection:
            self._execute('PUNSUBSCRIBE', *patterns)

    @in_loop_thread
    def subscribe(self, channel, *channels):
        return self._subscribe('SUBSCRIBE', channel, *channels)

    @in_loop_thread
    def unsubscribe(self, *channels):
        '''Un-subscribe from a list of ``channels``.
        '''
        if self._connection:
            self._execute('UNSUBSCRIBE', *channels)

    @in_loop_thread
    def close(self):
        '''Stop listening for messages.
        '''
        if self._connection:
            self._execute('PUNSUBSCRIBE')
            self._execute('UNSUBSCRIBE')

    ##    INTERNALS
    def _subscribe(self, *args):
        if not self._connection:
            protocol_factory = partial(PubsubProtocol, self,
                                       producer=self.store)
            self._connection = yield self.store.connect(protocol_factory)
            self._execute(*args)
        coroutine_return()

    def _execute(self, command, *args):
        chunk = self._connection.parser.multi_bulk(command, *args)
        self._connection._transport.write(chunk)
