from pulsar.utils.tools import get_unique_id

from .entity import MQ, Node


class BrokerConnection(MQ):
    '''A Broker Connection is a class which interfaces a remote broker.
It is a container in the message queue network which containes queues.'''
    pass


class Queue(Node):
    """\
A Queue declaration.

:keyword name: See :attr:`name`.
:keyword routing_key: See :attr:`routing_key`.
:keyword channel: See :attr:`channel`.
:keyword durable: See :attr:`durable`.
:keyword exclusive: See :attr:`exclusive`.
:keyword auto_delete: See :attr:`auto_delete`.
:keyword queue_arguments: See :attr:`queue_arguments`.
:keyword binding_arguments: See :attr:`binding_arguments`.

.. attribute:: name

    Name of the queue. Default is no name (default queue destination).

.. attribute:: exchange

    The :class:`Exchange` the queue binds to.

.. attribute:: routing_key

    The routing key (if any), also called *binding key*.

    The interpretation of the routing key depends on
    the :attr:`Exchange.type`.

        * direct exchange

            Matches if the routing key property of the message and
            the :attr:`routing_key` attribute are identical.

        * fanout exchange

            Always matches, even if the binding does not have a key.

        * topic exchange

            Matches the routing key property of the message by a primitive
            pattern matching scheme. The message routing key then consists
            of words separated by dots (`"."`, like domain names), and
            two special characters are available; star (`"*"`) and hash
            (`"#"`). The star matches any word, and the hash matches
            zero or more words. For example `"*.stock.#"` matches the
            routing keys `"usd.stock"` and `"eur.stock.db"` but not
            `"stock.nasdaq"`.

.. attribute:: channel

    The channel the Queue is bound to (if bound).

.. attribute:: durable

    Durable queues remain active when a server restarts.
    Non-durable queues (transient queues) are purged if/when
    a server restarts.
    Note that durable queues do not necessarily hold persistent
    messages, although it does not make sense to send
    persistent messages to a transient queue.

    Default is :const:`True`.

.. attribute:: exclusive

    Exclusive queues may only be consumed from by the
    current connection. Setting the 'exclusive' flag
    always implies 'auto-delete'.

    Default is :const:`False`.

.. attribute:: auto_delete

    If set, the queue is deleted when all consumers have
    finished using it. Last consumer can be cancelled
    either explicitly or because its channel is closed. If
    there was no consumer ever on the queue, it won't be
    deleted.

.. attribute:: queue_arguments

    Additional arguments used when declaring the queue.

.. attribute:: binding_arguments

    Additional arguments used when binding the queue.

"""
    routing_key = ""

    def __init__(self, name=None, routing_key=None, channel=None):
        self._name = name or get_unique_id()
        self.routing_key = routing_key or self.routing_key
        self.channel = channel

    def queue_declare(self, nowait=False, passive=False):
        """Declare queue on the server.

        :keyword nowait: Do not wait for a reply.
        :keyword passive: If set, the server will not create the queue.
            The client can use this to check whether a queue exists
            without modifying the server state.

        """
        return self.channel.queue_declare(queue=self.name,
                                          passive=passive,
                                          durable=self.durable,
                                          exclusive=self.exclusive,
                                          auto_delete=self.auto_delete,
                                          arguments=self.queue_arguments,
                                          nowait=nowait)

    def queue_bind(self, nowait=False):
        """Create the queue binding on the server.

        :keyword nowait: Do not wait for a reply.

        """
        return self.channel.queue_bind(queue=self.name,
                                       exchange=self.exchange.name,
                                       routing_key=self.routing_key,
                                       arguments=self.binding_arguments,
                                       nowait=nowait)

    def get(self, no_ack=None):
        """Poll the server for a new message.

        Returns the message instance if a message was available,
        or :const:`None` otherwise.

        :keyword no_ack: If set messages received does not have to
            be acknowledged.

        This method provides direct access to the messages in a
        queue using a synchronous dialogue, designed for
        specific types of applications where synchronous functionality
        is more important than performance.

        """
        message = self.channel.basic_get(queue=self.name, no_ack=no_ack)
        if message is not None:
            return self.channel.message_to_python(message)

    def purge(self, nowait=False):
        """Remove all messages from the queue."""
        return self.channel.queue_purge(queue=self.name, nowait=nowait) or 0

    def consume(self, consumer_tag=None, callback=None, no_ack=None,
            nowait=False):
        """Start a queue consumer.

        Consumers last as long as the channel they were created on, or
        until the client cancels them.

        :keyword consumer_tag: Unique identifier for the consumer. The
          consumer tag is local to a connection, so two clients
          can use the same consumer tags. If this field is empty
          the server will generate a unique tag.

        :keyword no_ack: If set messages received does not have to
            be acknowledged.

        :keyword nowait: Do not wait for a reply.

        :keyword callback: callback called for each delivered message

        """
        return self.channel.basic_consume(queue=self.name,
                                          no_ack=no_ack,
                                          consumer_tag=consumer_tag,
                                          callback=callback,
                                          nowait=nowait)

    def cancel(self, consumer_tag):
        """Cancel a consumer by consumer tag."""
        return self.channel.basic_cancel(consumer_tag)

    def delete(self, if_unused=False, if_empty=False, nowait=False):
        """Delete the queue.

        :keyword if_unused: If set, the server will only delete the queue
            if it has no consumers. A channel error will be raised
            if the queue has consumers.

        :keyword if_empty: If set, the server will only delete the queue
            if it is empty. If if's not empty a channel error will be raised.

        :keyword nowait: Do not wait for a reply.

        """
        return self.channel.queue_delete(queue=self.name,
                                         if_unused=if_unused,
                                         if_empty=if_empty,
                                         nowait=nowait)

    def unbind(self):
        """Delete the binding on the server."""
        return self.channel.queue_unbind(queue=self.name,
                                         exchange=self.exchange.name,
                                         routing_key=self.routing_key,
                                         arguments=self.binding_arguments)

