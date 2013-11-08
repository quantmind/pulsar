'''The :mod:`pulsar.apps.pubsub` module implements a middleware
handler for the Publish/Subscribe pattern. The middleware can be used to
synchronise pulsar actors across processes and machines.

.. epigraph::

    In software architecture, publish/subscribe is a messaging pattern where
    senders of messages, called publishers, do not program the messages to
    be sent directly to specific receivers, called subscribers.
    Instead, published messages are characterised into classes, without
    knowledge of what, if any, subscribers there may be. Similarly,
    subscribers express interest in one or more classes, and only receive
    messages that are of interest, without knowledge of what, if any,
    publishers there are.

    -- wikipedia_


When using this middleware, one starts by creating a :class:`PubSub` handler::

    from pulsar.apps.pubsub import PubSub

    pubsub = PubSub(backend=None, ...)

The ``backend`` parameter is needed in order to select the backend
to use. If not supplied, the default ``local://`` backend is used.

Usage
==============

A pubsub handler can be passed to different process domain and therefore it
can be used to synchronise pulsar actors.

A typical usage is when one needs to serve a websocket on a multiprocessing
web server such as the :ref:`pulsar WSGI server <apps-wsgi>`.
For example, the :ref:`websocket chat server <tutorials-chat>` uses a pubsub
handler to propagate a message received from an http client to all
the clients currently listening for messages.

Since these clients may be served by different web server processes, the
pubsub handler is an handy tool for synchronisation.

Registering a client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a new client connects to the websocket, the ``on_open`` method of a
:ref:`websocket handler <apps-ws>` add the websocket to the set of
clients of the ``pubsub`` handler::

    def on_open(self, websocket):
        self.pubsub.add_client(PubSubClient(websocket))

The ``PubSubClient`` is a :class:`Client` wrapper around the ``websocket``
which calls the ``websocket`` write method when called::

    class PubSubClient(pubsub.Client):

    def __init__(self, connection):
        self.connection = connection

    def __call__(self, channel, message):
        if channel == 'webchat':
            self.connection.write(message)


Publishing
~~~~~~~~~~~~~~~~~~~~~~

When a new message is received by the websocket ``on_message`` method,
we use the ``pubsub`` handle to publish the message::

    def on_message(self, websocket, msg):
        self.pubsub.publish('webchat', msg)


And the rest is taken care of by the callables ``PubSubClient`` which write
the message to all listening websocket clients.


PubSub handler
========================

.. autoclass:: PubSub
   :members:
   :member-order: bysource


PubSub Client
======================

.. autoclass:: Client
   :members:
   :member-order: bysource


PubSub backend
========================

.. autoclass:: PubSubBackend
   :members:
   :member-order: bysource


.. _wikipedia: http://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern
'''
import pulsar
from pulsar.utils.pep import to_string

from .client import create_store


def pubsub(store_or_url, protocol=None):
    store = create_store(store_or_url)
    return PubSub(store, protocol)


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


class PubSub(object):
    '''Publish/Subscribe paradigm handler.

    .. attribute:: backend

        The :class:`PubSubBackend` for this handler.

    .. attribute:: encoder

        Optional callable which encode the messages before they are published.


    A backend handler is a picklable instance and therefore it can be
    passed to different process domains.
    '''
    def __init__(self, store, protocol=None):
        self.store = store
        self._loop = store._loop
        self._pubsub = store.pubsub()
        self._pubsub.bind_event('on_message', self._broadcast)
        self._protocol = protocol
        self._clients = set()

    @property
    def dns(self):
        return self.store.dns

    def add_client(self, client):
        '''Add a new ``client`` to the set of all :attr:`clients`.

        Clients must be callable. When a new message is received
        from the publisher, the :meth:`broadcast` method will notify all
        :attr:`clients` via the ``callable`` method.'''
        self._clients.add(client)

    def remove_client(self, client):
        '''Remove *client* from the set of all :attr:`clients`.'''
        self._clients.discard(client)

    def publish(self, channel, message):
        '''Publish a ``message`` to ``channel``.'''
        if self._protocol:
            message = self._protocol.encode(message)
        return self._pubsub.publish(channel, message)

    def subscribe(self, *channels):
        '''Subscribe to ``channels``.'''
        return self._pubsub.subscribe(*channels)

    def unsubscribe(self, *channels):
        '''Unsubscribe from ``channels``.'''
        return self._pubsub.unsubscribe(*channels)

    def psubscribe(self, *patterns):
        '''Subscribe to channels ``patterns``.'''
        return self._pubsub.psubscribe(*channels)

    def punsubscribe(self, *patterns):
        '''Unsubscribe from channel ``patterns``.'''
        return self._pubsub.unsubscribe(*channels)

    def close(self):
        '''Close connections'''
        return self._pubsub.close()

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
