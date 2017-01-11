import re
import logging
import json
from functools import wraps
from enum import Enum
from asyncio import gather
from collections import namedtuple, OrderedDict

from ...utils.exceptions import ProtocolError
from ...apps.ds import redis_to_py_pattern
from .store import PubSubClient


event_callbacks = namedtuple('event_callbacks', 'name pattern regex callbacks')


LOGGER = logging.getLogger('pulsar.channels')


class StatusType(Enum):
    initialised = 1
    connecting = 2
    connected = 3
    disconnected = 4
    closed = 5


can_connect = frozenset((StatusType.initialised, StatusType.disconnected))


RECONNECT_LAG = 2
DEFAULT_NAMESPACE = ''
DEFAULT_CHANNEL = 'server'


def backoff(value):
    return min(value + 0.25, 16)


class CallbackError(Exception):
    """Exception which allow for a clean callback removal
    """


class Json:

    def encode(self, msg):
        return json.dumps(msg)

    def decode(self, msg):
        if isinstance(msg, bytes):
            msg = msg.decode('utf-8')
        try:
            return json.loads(msg)
        except Exception:
            raise ProtocolError('Invalid JSON') from None


class Connector:
    namespace_delimiter = '_'

    def __init__(self, store, namespace=None):
        self.connection_error = False
        self.namespace = (
            namespace or
            store.urlparams.get('namespace') or
            DEFAULT_NAMESPACE
        ).lower()
        if (self.namespace and not
                self.namespace.endswith(self.namespace_delimiter)):
            self.namespace = '%s%s' % (
                self.namespace, self.namespace_delimiter
            )
        self.dns = store.buildurl(namespace=self.namespace)

    def __repr__(self):
        return self.dns

    def __str__(self):
        return self.__repr__()

    def prefixed(self, name):
        if self.namespace and not name.startswith(self.namespace):
            name = '%s%s' % (self.namespace, name)
        return name

    def connection_ok(self):
        if self.connection_error:
            self.logger.warning(
                'connection with %s established - all good',
                self
            )
            self.connection_error = False
        else:
            return True


class Channels(Connector, PubSubClient):
    """Manage channels for publish/subscribe
    """
    statusType = StatusType

    def __init__(self, store, namespace=None, status_channel=None,
                 logger=None):
        super().__init__(store, namespace=namespace)
        self.store = store
        self.channels = OrderedDict()
        self.logger = logger or LOGGER
        self.status_channel = self.channel(status_channel or DEFAULT_CHANNEL)
        self.status = self.statusType.initialised

    @property
    def _loop(self):
        return self.store._loop

    def __repr__(self):
        return self.dns

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

    async def register(self, channel, event, callback):
        """Register a callback to ``channel_name`` and ``event``.

        A prefix will be added to the channel name if not already available or
        the prefix is an empty string

        :param channel: channel name
        :param event: event name
        :param callback: callback to execute when event on channel occurs
        :return: a coroutine which results in the channel where the callback
            was registered
        """
        channel = self.channel(channel)
        event = channel.register(event, callback)
        await channel.connect(event.name)
        return channel

    async def unregister(self, channel, event, callback):
        """Safely unregister a callback from the list of ``event``
        callbacks for ``channel_name``.

        :param channel: channel name
        :param event: event name
        :param callback: callback to execute when event on channel occurs
        :return: a coroutine which results in the channel object where the
            ``callback`` was removed (if found)
        """
        channel = self.channel(channel, create=False)
        if channel:
            channel.unregister(event, callback)
            if not channel:
                await channel.disconnect()
                self.channels.pop(channel.name)
        return channel

    async def connect(self, next_time=None):
        """Connect with store

        :return: a coroutine and therefore it must be awaited
        """
        if self.status in can_connect:
            loop = self._loop
            if loop.is_running():
                self.status = StatusType.connecting
                await self._connect(next_time)

    async def publish(self, channel, event, data=None):
        """Publish a new ``event`` on a ``channel``
        :param channel: channel name
        :param event: event name
        :param data: optional payload to include in the event
        :return: a coroutine and therefore it must be awaited
        """
        raise NotImplementedError

    async def _subscribe(self, channel, event=None):
        """Subscribe to the remote server
        """
        raise NotImplementedError

    async def _unsubscribe(self, channel):
        raise NotImplementedError

    async def close(self):
        """Close channels and underlying store handler

        :return: a coroutine and therefore it must be awaited
        """
        self.status = self.statusType.closed

    def channel(self, name, create=True):
        name = name.lower()
        channel = self.channels.get(name)
        if channel is None and create:
            channel = Channel(self, name)
            self.channels[channel.name] = channel
        return channel

    def event_pattern(self, event):
        """Channel pattern for an event name
        """
        return redis_to_py_pattern(event)

    def _connection_lost(self, *args):
        self.status = self.statusType.disconnected
        self._loop.create_task(self.connect())

    async def _connect(self, next_time):
        try:
            # register
            self.status = StatusType.connecting
            await self._subscribe(self.status_channel)
            self.status = StatusType.connected
            self.logger.warning(
                '%s ready and listening for events on %s - all good',
                self,
                self.status_channel.name
            )
        except ConnectionError:
            self.status = StatusType.disconnected
            next_time = backoff(next_time) if next_time else RECONNECT_LAG
            self.logger.critical(
                '%s cannot subscribe - connection error - '
                'try again in %s seconds',
                self,
                next_time
            )
            self._loop.call_later(
                next_time,
                lambda: self._loop.create_task(self.connect(next_time))
            )
        else:
            await gather(*[c.connect() for c in self.channels.values()
                           if c.name != self.status_channel.name])


def safe_execution(method):

    @wraps(method)
    async def _(self, *args, **kwargs):
        try:
            await method(self, *args, **kwargs)
        except ConnectionError:
            self.channels.status = StatusType.disconnected
            await self.channels.connect()

    return _


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
        self.callbacks = OrderedDict()

    @property
    def events(self):
        """List of event names this channel is registered with
        """
        return tuple((e.name for e in self.callbacks.values()))

    def __repr__(self):
        return repr(self.callbacks)

    def __len__(self):
        return len(self.callbacks)

    def __contains__(self, pattern):
        return pattern in self.callbacks

    def __iter__(self):
        return iter(self.channels.values())

    def __call__(self, message):
        event = message.pop('event', '')
        data = message.get('data')
        self.fire(event, data)

    def fire(self, event, data=None):
        for entry in tuple(self.callbacks.values()):
            match = entry.regex.match(event)
            if match:
                match = match.group()
                for callback in tuple(entry.callbacks):
                    try:
                        callback(self, match, data)
                    except CallbackError:
                        self._remove_callback(entry, callback)
                    except Exception:
                        self._remove_callback(entry, callback)
                        self.channels.logger.exception(
                            'callback exception: channel "%s" event "%s"',
                            self.name, event)

    @safe_execution
    async def connect(self, event=None):
        channels = self.channels
        if channels.status == StatusType.connected:
            await self.channels._subscribe(self, event)

    @safe_execution
    async def disconnect(self):
        channels = self.channels
        if channels.status == StatusType.connected:
            await self.channels._unsubscribe(self)

    def register(self, event, callback):
        """Register a ``callback`` for ``event``
        """
        pattern = self.channels.event_pattern(event)
        entry = self.callbacks.get(pattern)
        if not entry:
            entry = event_callbacks(event, pattern, re.compile(pattern), [])
            self.callbacks[entry.pattern] = entry

        if callback not in entry.callbacks:
            entry.callbacks.append(callback)

        return entry

    def unregister(self, event, callback):
        pattern = self.channels.event_pattern(event)
        entry = self.callbacks.get(pattern)
        if entry:
            return self._remove_callback(entry, callback)

    def _remove_callback(self, entry, callback):
        if callback in entry.callbacks:
            entry.callbacks.remove(callback)
            if not entry.callbacks:
                self.callbacks.pop(entry.pattern)
            return entry
