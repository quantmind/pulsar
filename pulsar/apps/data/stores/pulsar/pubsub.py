from functools import partial

from pulsar import in_loop_thread, Protocol, EventHandler, coroutine_return


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


class PubSubClient(object):
    '''Interface for a client of :class:`PubSub` handler.

    Instances of this :class:`Client` are callable object and are
    called once a new message has arrived from a subscribed channel.
    The callable accepts two parameters:

    * ``channel`` the channel which originated the message
    * ``message`` the message
    '''
    def __call__(self, channel, message):
        raise NotImplementedError


class PubSub(EventHandler):
    '''Asynchronous Publish/Subscriber handler for pulsar and redis stores.

    To listen for messages you can bind to the ``on_message`` event::

        pubsub = client.pubsub()
        pubsub.bind_event('on_message', handle_messages)
        pubsub.subscribe('mychannel')

    You can bind as many handlers to the ``on_message`` event as you like.
    The handlers receive one parameter only, a two-elements tuple
    containing the ``channel`` and the ``message``.
    '''
    MANY_TIMES_EVENTS = ('on_message',)

    def __init__(self, store, protocol=None):
        super(PubSub, self).__init__()
        self.store = store
        self._loop = store._loop
        self._protocol = protocol
        self._connection = None
        self._clients = set()
        self.bind_event('on_message', self._broadcast)

    def publish(self, channel, message):
        '''Publish a new ``message`` to a ``channel``.

        This method return a pulsar Deferred which results in the number of
        subscribers that will receive the message (the same behaviour as
        redis publish command).
        '''
        return self.store.execute('PUBLISH', channel, message)

    def add_client(self, client):
        '''Add a new ``client`` to the set of all :attr:`clients`.

        Clients must be callable. When a new message is received
        from the publisher, the :meth:`broadcast` method will notify all
        :attr:`clients` via the ``callable`` method.'''
        self._clients.add(client)

    def remove_client(self, client):
        '''Remove *client* from the set of all :attr:`clients`.'''
        self._clients.discard(client)

    def count(self, *channels):
        '''Returns the number of subscribers (not counting clients
        subscribed to patterns) for the specified channels.
        '''
        return self.store.execute('PUBSUB', 'NUMSUB', *channels)

    def channels(self, pattern=None):
        '''Lists the currently active channels.

        An active channel is a Pub/Sub channel with one ore more subscribers
        (not including clients subscribed to patterns).
        If no ``pattern`` is specified, all the channels are listed,
        otherwise if ``pattern`` is specified only channels matching the
        specified glob-style pattern are listed.
        '''
        if pattern:
            return self.store.execute('PUBSUB', 'CHANNELS', pattern)
        else:
            return self.store.execute('PUBSUB', 'CHANNELS')

    @in_loop_thread
    def psubscribe(self, pattern, *patterns):
        '''Subscribe to a list of ``patterns``.
        '''
        return self._subscribe('PSUBSCRIBE', pattern, *patterns)

    @in_loop_thread
    def punsubscribe(self, *channels):
        '''Unsubscribe from a list of ``patterns``.
        '''
        if self._connection:
            self._execute('PUNSUBSCRIBE', *patterns)

    @in_loop_thread
    def subscribe(self, channel, *channels):
        '''Subscribe to a list of ``channels``.
        '''
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

    def _broadcast(self, response):
        '''Broadcast ``message`` to all :attr:`clients`.'''
        remove = set()
        channel = to_string(response[0])
        message = response[1]
        if self._protocol:
            message = self._protocol.dencode(message)
        for client in self._clients:
            try:
                client(channel, message)
            except IOError:
                remove.add(client)
            except Exception:
                self._loop.logger.exception(
                    'Exception while processing pub/sub client. Removing it.')
                remove.add(client)
        self._clients.difference_update(remove)
