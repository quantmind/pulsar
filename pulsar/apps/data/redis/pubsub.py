from ....utils.lib import ProtocolConsumer
from ..store import PubSub, PubSubClient
from ..channels import Channels


class PubsubProtocolConsumer(ProtocolConsumer):

    def feed_data(self, data):
        parser = self.connection.parser
        parser.feed(data)
        response = parser.get()
        while response is not False:
            if not isinstance(response, Exception):
                if isinstance(response, list):
                    command = response[0]
                    if command == b'message':
                        response = response[1:3]
                        self.producer.broadcast(response)
                    elif command == b'pmessage':
                        response = response[2:4]
                        self.producer.broadcast(response)
            else:
                raise response
            response = parser.get()


class RedisPubSub(PubSub):
    """Asynchronous Publish/Subscriber handler for pulsar and redis stores.
    """
    push_connection = None

    def publish(self, channel, message):
        if self.protocol:
            message = self.protocol.encode(message)
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

    # SUBSCRIBE CONNECTION
    def psubscribe(self, pattern, *patterns):
        return self._subscribe('PSUBSCRIBE', pattern, *patterns)

    def punsubscribe(self, *patterns):
        self._execute('PUNSUBSCRIBE', *patterns)

    def subscribe(self, channel, *channels):
        return self._subscribe('SUBSCRIBE', channel, *channels)

    def unsubscribe(self, *channels):
        '''Un-subscribe from a list of ``channels``.
        '''
        return self._execute('UNSUBSCRIBE', *channels)

    async def close(self):
        '''Stop listening for messages.
        '''
        if self.push_connection:
            self._execute('PUNSUBSCRIBE')
            self._execute('UNSUBSCRIBE')
            await self.push_connection.close()

    #    INTERNALS
    def _execute(self, *args):
        if self.push_connection:
            data = self.push_connection.parser.multi_bulk(args)
            self.push_connection.write(data)

    async def _subscribe(self, *args):
        if not self.push_connection:
            self.push_connection = await self.store.connect(
                self.create_protocol)
            self.push_connection.upgrade(PubsubProtocolConsumer)
            self.push_connection.event('connection_lost').bind(self._conn_lost)
        self._execute(*args)

    def _conn_lost(self, _, **kw):
        self.push_connection = None


class RedisChannels(Channels, PubSubClient):
    """Manage redis channels-events
    """
    def __init__(self, pubsub, **kw):
        assert pubsub.protocol, "protocol required for channels"
        super().__init__(pubsub.store, **kw)
        self.pubsub = pubsub
        self.pubsub.event('connection_lost').bind(self._connection_lost)
        self.pubsub.add_client(self)

    def lock(self, name, **kwargs):
        """Global distributed lock
        """
        return self.pubsub.store.client().lock(self.prefixed(name), **kwargs)

    async def publish(self, channel, event, data=None):
        """Publish a new ``event`` on a ``channel``

        :param channel: channel name
        :param event: event name
        :param data: optional payload to include in the event
        :return: a coroutine and therefore it must be awaited
        """
        msg = {'event': event, 'channel': channel}
        if data:
            msg['data'] = data
        try:
            await self.pubsub.publish(self.prefixed(channel), msg)
        except ConnectionRefusedError:
            self.connection_error = True
            self.logger.critical(
                '%s cannot publish on "%s" channel - connection error',
                self,
                channel
            )
        else:
            self.connection_ok()

    async def _subscribe(self, channel, event=None):
        channel_name = self.prefixed(channel.name)
        await self.pubsub.subscribe(channel_name)

    async def _unsubscribe(self, channel, event=None):
        channel_name = self.prefixed(channel.name)
        await self.pubsub.unsubscribe(channel_name)

    async def close(self):
        """Close channels and underlying pubsub handler

        :return: a coroutine and therefore it must be awaited
        """
        push_connection = self.pubsub.push_connection
        self.status = self.statusType.closed
        if push_connection:
            push_connection.event('connection_lost').unbind(
                self._connection_lost
            )
            await self.pubsub.close()
