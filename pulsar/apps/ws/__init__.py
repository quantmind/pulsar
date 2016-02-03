'''The :mod:`pulsar.apps.ws` contains WSGI_ middleware for
handling the WebSocket_ protocol.
Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`.WebSocket` for the handshake_ and a class derived from
:class:`.WS` handler for the communication part.

This is a Web Socket handler which echos all received messages
back to the client::

    from pulsar.apps import wsgi, ws

    class EchoWS(ws.WS):

        def on_message(self, websocket, message):
            websocket.write(message)


To create a valid :class:`.WebSocket` middleware initialise as follow::

    wm = ws.WebSocket('/bla', EchoWS())
    app = wsgi.WsgiHandler(middleware=(..., wm))
    wsgi.WSGIServer(callable=app).start()


.. _WSGI: http://www.python.org/dev/peps/pep-3333/
.. _WebSocket: http://tools.ietf.org/html/rfc6455
.. _handshake: http://tools.ietf.org/html/rfc6455#section-1.3

API
==============

.. _websocket-middleware:

.. _websocket-handler:

WebSocket handler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WS
   :members:
   :member-order: bysource

.. module:: pulsar.apps.ws.websocket

WebSocket
~~~~~~~~~~~~~~~~

.. autoclass:: WebSocket
   :members:
   :member-order: bysource

WebSocket protocol
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WebSocketProtocol
   :members:
   :member-order: bysource

'''
import logging

from pulsar.apps import data

from .websocket import WebSocket, WebSocketProtocol


__all__ = ['WebSocket', 'WebSocketProtocol', 'WS']


LOGGER = logging.getLogger('pulsar.ws')


class WS:
    '''A web socket handler for both servers and clients.

    It implements the asynchronous message passing for a
    :class:`.WebSocketProtocol`.
    On the server, the communication is started by the
    :class:`.WebSocket` middleware after a successful handshake.

    Override :meth:`on_message` to handle incoming string messages and
    :meth:`on_bytes` to handle incoming ``bytes`` messages.

    You can also override :meth:`on_open` and :meth:`on_close` to perform
    specific tasks when the websocket is opened or closed.

    These methods accept as first parameter the
    :class:`.WebSocketProtocol` created during the handshake.
    '''
    frame_parser = None

    def on_open(self, websocket):
        '''Invoked when a new ``websocket`` is opened.

        A web socket is opened straight after the upgrade headers are
        sent (servers) or received (clients).
        '''
        pass

    def on_message(self, websocket, message):
        '''Handles incoming messages on the WebSocket.

        This method should be overwritten
        '''
        pass

    def on_bytes(self, websocket, body):
        '''Handles incoming bytes.'''
        pass

    def on_ping(self, websocket, message):
        '''Handle incoming ping ``message``.

        By default it writes back the message as a ``pong`` frame.
        '''
        websocket.pong(message)

    def on_pong(self, websocket, body):
        '''Handle incoming pong ``message``.'''
        pass

    def on_close(self, websocket):
        """Invoked when the ``websocket`` is closed.
        """
        pass


class PubSubClient(data.PubSubClient):
    __slots__ = ('connection', 'channel')

    def __init__(self, websocket, channel):
        self.websocket = websocket
        self.channel = channel

    def __call__(self, channel, message):
        handler = self.websocket.handler
        handler.write(self.websocket, message)


class PubSubWS(WS):
    '''A :class:`.WS` handler with a publish-subscribe handler
    '''
    client = PubSubClient

    def __init__(self, pubsub, channel):
        self.pubsub = pubsub
        self.channel = channel

    def on_open(self, websocket):
        '''When a new websocket connection is established it creates a
        new :class:`ChatClient` and adds it to the set of clients of the
        :attr:`pubsub` handler.'''
        LOGGER.info('New websocket opened. Add client to %s on "%s" channel',
                    self.pubsub, self.channel)
        self.pubsub.add_client(self.client(websocket, self.channel))

    def write(self, websocket, message):
        websocket.write(message)
