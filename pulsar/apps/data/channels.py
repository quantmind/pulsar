import re
import logging
import json
from enum import Enum
from asyncio import gather
from collections import namedtuple, OrderedDict

from pulsar import ProtocolError
from pulsar.apps.ds import redis_to_py_pattern

from .store import PubSubClient


regex_callbacks = namedtuple('regex_callbacks', 'regex callbacks')


LOGGER = logging.getLogger('pulsar.channels')


class StatusType(Enum):
    initialised = 1
    connecting = 2
    connected = 3
    disconnected = 4
    closed = 5


can_connect = frozenset((StatusType.initialised, StatusType.disconnected))


RECONNECT_LAG = 2
DEFAULT_NAMESPACE = 'pulsar'
DEFAULT_CHANNEL = 'server'


def backoff(value):
    return min(value + 0.25, 16)


class Json:

    def encode(self, msg):
        return json.dumps(msg)

    def decode(self, msg):
        try:
            return json.loads(msg.decode('utf-8'))
        except Exception as exc:
            raise ProtocolError('Invalid JSON') from exc


class Channels(PubSubClient):
    """Manage channels for publish/subscribe
    """

    def __init__(self, pubsub, namespace=None, status_channel=None,
                 logger=None):
        assert pubsub.protocol, "protocol required for channels"
        self.channels = OrderedDict()
        self.logger = logger or LOGGER
        self.namespace = '%s_' % (namespace or DEFAULT_NAMESPACE).lower()
        self.status_channel = (status_channel or DEFAULT_CHANNEL).lower()
        self.status = StatusType.initialised
        self.pubsub = pubsub
        self.pubsub.bind_event('connection_lost', self._connection_lost)
        self.pubsub.add_client(self)

    @property
    def _loop(self):
        return self.pubsub._loop

    def __repr__(self):
        return repr(self.channels)

    def __len__(self):
        return len(self.channels)

    def __contains__(self, name):
        return name in self.channels

    def __iter__(self):
        return iter(self.channels.values())

    def __call__(self, channel_name, message):
        if channel_name.startswith(self.namespace):
            name = channel_name[len(self.namespace):]
            channel = self.channels.get(name)
            if channel:
                channel(message)

    async def connect(self, next_time=None):
        if self.status in can_connect:
            loop = self._loop
            if loop.is_running():
                self.status = StatusType.connecting
                await self._connect(next_time)

    async def register(self, channel_name, event, callback):
        """Register a callback to ``channel_name`` and ``event``
        A prefix will be added to the channel name if not already available or
        the prefix is an empty string
        :param channel_name: channel name
        :param event: event name
        :param callback: callback to execute when event on channel occurs
        :return: the list of channels subscribed
        """
        name = channel_name.lower()
        channel = self.channels.get(name)
        if channel is None:
            channel = self.create_channel(name)
            self.channels[channel.name] = channel
            await channel.connect()
        channel.register(event, callback)

    async def close(self):
        self.pubsub.remove_callback('connection_lost', self._connection_lost)
        self.status = StatusType.closed
        await self.pubsub.close()

    def publish(self, channel, event, data=None):
        """Publish a new ``event` on a ``channel``
        :param channel: channel name
        :param event: event name
        :param data: optional payload to include in the event
        :param user: optional user to include in the event
        """
        msg = {'event': event, 'channel': channel}
        if data:
            msg['data'] = data
        channel = self.prefixed(channel)
        return self.pubsub.publish(channel, msg)

    def prefixed(self, name):
        if not name.startswith(self.namespace):
            name = '%s%s' % (self.namespace, name)
        return name

    def create_channel(self, name):
        return Channel(self, name)

    def _connection_lost(self, *args):
        self.status = StatusType.disconnected
        self.fire_event('connection_lost')
        self.connect()

    async def _connect(self, next_time):
        try:
            # register
            self.status = StatusType.connecting
            await self.pubsub.subscribe(self.prefixed(self.status_channel))
            self.status = StatusType.connected
            self.logger.warning(
                '%s ready and listening for events on %s - all good',
                self,
                self.status_channel
            )
            await gather(*[c.connect() for c in self.channels.values()])
        except ConnectionRefusedError:
            next_time = backoff(next_time) if next_time else RECONNECT_LAG
            self.logger.critical(
                '%s cannot subscribe - connection error - '
                'try again in %s seconds',
                self,
                next_time
            )
            self._loop.call_later(next_time,
                                  self._loop.make_task,
                                  self.connect(next_time))


class Channel:
    """Channel
    .. attribute:: channels
        the channels container
    .. attribute:: name
        channel name
    .. attribute:: callbacks
        dictionary mapping events to callbacks
    """
    def __init__(self, channels, name):
        self.channels = channels
        self.name = name
        self.callbacks = {}

    def __repr__(self):
        return repr(self.callbacks)

    def __call__(self, message):
        event = message.pop('event', '')
        data = message.get('data')
        for regex, callbacks in self.callbacks.values():
            match = regex.match(event)
            if match:
                match = match.group()
                for callback in callbacks:
                    callback(self, match, data)

    async def connect(self):
        channels = self.channels
        if channels.status == StatusType.connected:
            channel_name = channels.prefixed(self.name)
            await self.channels.pubsub.subscribe(channel_name)

    def register(self, event, callback):
        """Register a ``callback`` for ``event``
        """
        regex = redis_to_py_pattern(event)
        entry = self.callbacks.get(regex)
        if not entry:
            entry = regex_callbacks(re.compile(regex), [])
            self.callbacks[regex] = entry

        if callback not in entry.callbacks:
            entry.callbacks.append(callback)

        return entry
