from itertools import count
 
from pulsar import ispy3k, Empty

from .entity import MQ
from .message import Message


class Channel(MQ):
    """A channel in the message queue network.

    :param connection: The transport instance this channel is part of.

    """
    #: message class used.
    Message = Message

    #: flag to restore unacked messages when channel
    #: goes out of scope.
    do_restore = True

    #: counter used to generate delivery tags for this channel.
    if ispy3k:
        _next_delivery_tag = count(1).__next__
    else:
        _next_delivery_tag = count(1).next

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self._consumers = set()
        self._cycle = None
        self._tag_to_queue = {}
        self._qos = None
        self.closed = False

    def queue_create(self, queue, **kwargs):
        """Declare queue."""
        self._new_queue(queue, **kwargs)
        return queue, self._size(queue), 0

    def queue_delete(self, queue):
        """Delete queue."""
        self._delete(queue)
        self.state.bindings.pop(queue, None)

    def queue_purge(self, queue, **kwargs):
        """Remove all ready messages from queue."""
        return self._purge(queue)

    def basic_publish(self, message, exchange, routing_key, **kwargs):
        """Publish message."""
        message["properties"]["delivery_info"]["exchange"] = exchange
        message["properties"]["delivery_info"]["routing_key"] = routing_key
        message["properties"]["delivery_tag"] = self._next_delivery_tag()
        for queue in self._lookup(exchange, routing_key):
            self._put(queue, message, **kwargs)

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Consume from `queue`"""
        self._tag_to_queue[consumer_tag] = queue

        def _callback(raw_message):
            message = self.Message(self, raw_message)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return callback(message)

        self.connection._callbacks[queue] = _callback
        self._consumers.add(consumer_tag)
        self._reset_cycle()

    def basic_cancel(self, consumer_tag):
        """Cancel consumer by consumer tag."""
        self._consumers.remove(consumer_tag)
        self._reset_cycle()
        queue = self._tag_to_queue.pop(consumer_tag, None)
        self.connection._callbacks.pop(queue, None)

    def basic_get(self, queue, **kwargs):
        """Get message by direct access (synchronous)."""
        try:
            return self._get(queue)
        except Empty:
            pass

    def basic_ack(self, delivery_tag):
        """Acknowledge message."""
        self.qos.ack(delivery_tag)

    def basic_recover(self, requeue=False):
        """Recover unacked messages."""
        if requeue:
            return self.qos.restore_unacked()
        raise NotImplementedError("Does not support recover(requeue=False)")

    def basic_reject(self, delivery_tag, requeue=False):
        """Reject message."""
        self.qos.reject(delivery_tag, requeue=requeue)

    def basic_qos(self, prefetch_size=0, prefetch_count=0,
            apply_global=False):
        """Change QoS settings for this channel.

        Only `prefetch_count` is supported.

        """
        self.qos.prefetch_count = prefetch_count

    def _lookup(self, exchange, routing_key, default="ae.undeliver"):
        """Find all queues matching `routing_key` for the given `exchange`.

        Returns `default` if no queues matched.

        """
        try:
            table = self.get_table(exchange)
            return self.typeof(exchange).lookup(table, exchange,
                                                routing_key, default)
        except KeyError:
            self._new_queue(default)
            return [default]

    def _restore(self, message):
        """Redeliver message to its original destination."""
        delivery_info = message.delivery_info
        message = message.serializable()
        message["redelivered"] = True
        for queue in self._lookup(delivery_info["exchange"],
                                  delivery_info["routing_key"]):
            self._put(queue, message)

    def drain_events(self, timeout=None):
        if self._consumers and self.qos.can_consume():
            if hasattr(self, "_get_many"):
                return self._get_many(self._active_queues, timeout=timeout)
            return self._poll(self.cycle, timeout=timeout)
        raise Empty()

    def message_to_python(self, raw_message):
        """Convert raw message to :class:`Message` instance."""
        if not isinstance(raw_message, self.Message):
            return self.Message(self, payload=raw_message)
        return raw_message

    def prepare_message(self, message_data, priority=None,
                        content_type=None, content_encoding=None, headers=None,
                        properties=None):
        """Prepare message data."""
        properties = properties or {}
        info = properties.setdefault("delivery_info", {})
        info["priority"] = priority or 0

        return {"body": message_data,
                "content-encoding": content_encoding,
                "content-type": content_type,
                "headers": headers or {},
                "properties": properties or {}}

    def flow(self, active=True):
        """Enable/disable message flow.

        :raises NotImplementedError: as flow
            is not implemented by the base virtual implementation.

        """
        raise NotImplementedError("virtual channels does not support flow.")

    def close(self):
        """Close channel, cancel all consumers, and requeue unacked
        messages."""
        self.closed = True
        for consumer in list(self._consumers):
            self.basic_cancel(consumer)
        self.qos.restore_unacked_once()
        self.connection.close_channel(self)
        self._cycle = None

    def _reset_cycle(self):
        self._cycle = FairCycle(self._get, self._active_queues, Empty)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    @property
    def state(self):
        """Broker state containing exchanges and bindings."""
        return self.connection.state

    @property
    def qos(self):
        """:class:`QoS` manager for this channel."""
        if self._qos is None:
            self._qos = QoS(self)
        return self._qos

    @property
    def _active_queues(self):
        return [self._tag_to_queue[tag] for tag in self._consumers]

    @property
    def cycle(self):
        if self._cycle is None:
            self._reset_cycle()
        return self._cycle

    def _get(self, queue, timeout=None):
        """Get next message from `queue`."""
        raise NotImplementedError("Virtual channels must implement _get")

    def _put(self, queue, message):
        """Put `message` onto `queue`."""
        raise NotImplementedError("Virtual channels must implement _put")

    def _purge(self, queue):
        """Remove all messages from `queue`."""
        raise NotImplementedError("Virtual channels must implement _purge")

    def _size(self, queue):
        """Return the number of messages in `queue` as an :class:`int`."""
        return 0

    def _delete(self, queue):
        """Delete `queue`.

        This just purges the queue, if you need to do more you can
        override this method.

        """
        self._purge(queue)

    def _new_queue(self, queue, **kwargs):
        """Create new queue.

        Some implementations needs to do additiona actions when
        the queue is created.  You can do so by overriding this
        method.

        """
        pass

    def _poll(self, cycle, timeout=None):
        """Poll a list of queues for available messages."""
        return cycle.get()


class Transport(MQ):
    """\
Base class for transports

.. attribute: broker

    instance of :class:`pulsar.mq.BrokerConnection` owning this transport instance.
"""
    Channel = Channel
    
    #: Tuple of errors that can happen due to connection failure.
    connection_errors = ()

    #: Tuple of errors that can happen due to channel/method failure.
    channel_errors = ()

    def __init__(self, connection, **kwargs):
        self.client = connection
        self.channels = []
        self._callbacks = {}

    def establish_connection(self):
        raise NotImplementedError("Subclass responsibility")

    def close_connection(self, connection):
        '''close the connection by closing all opened channels'''
        while self.channels:
            try:
                channel = self.channels.pop()
            except KeyError:    # pragma: no cover
                pass
            else:
                channel.close()

    def create_channel(self, connection):
        channel = self.Channel(connection)
        self.channels.append(channel)
        return channel

    def close_channel(self, channel):
        try:
            self.channels.remove(channel)
        except ValueError:
            pass

    def drain_events(self, connection, timeout=None):
        if not self.channels:
            raise ValueError("No channels to drain events from.")
        loop = 0
        while 1:
            try:
                item, channel = self.cycle.get()
            except Empty:
                if timeout and loop * 0.1 >= timeout:
                    raise socket.timeout()
                loop += 1
                sleep(0.1)
            else:
                break

        message, queue = item

        if not queue or queue not in self._callbacks:
            raise KeyError(
                "Received message for queue '%s' without consumers: %s" % (
                    queue, message))

        self._callbacks[queue](message)

    def _drain_channel(self, channel):
        return channel.drain_events()

