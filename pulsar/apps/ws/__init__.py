'''The :mod:`pulsar.apps.ws` contains WSGI_ middleware for
handling the WebSocket_ protocol.
Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`WebSocket` for the handshake_ and a class derived from
:class:`WS` handler for the communication part.

This is a Web Socket handler which echos all received messages
back to the client::

    from pulsar.apps import wsgi, ws
    
    class EchoWS(ws.WS):
    
        def on_message(self, websocket, message):
            websocket.write(message)
            
            
To create a valid :class:`WebSocket` middleware initialise as follow::

    wm = ws.WebSocket('/bla', EchoWS())
    app = wsgi.WsgiHandler(middleware=(..., wm))
    wsgi.WSGIServer(callable=app).start()


.. _WSGI: http://www.python.org/dev/peps/pep-3333/
.. _WebSocket: http://tools.ietf.org/html/rfc6455
.. _handshake: http://tools.ietf.org/html/rfc6455#section-1.3

API
==============

.. _websocket-middleware:

WebSocket
~~~~~~~~~~~~~~~~

.. autoclass:: WebSocket
   :members:
   :member-order: bysource


.. _websocket-handler:

WebSocket handler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WS
   :members:
   :member-order: bysource
   

WebSocket protocol
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WebSocketProtocol
   :members:
   :member-order: bysource
'''
from .websocket import WebSocket, WebSocketProtocol
from .xhr import SocketIO


class WS(object):
    '''A web socket handler for both servers and clients. It implements
the asynchronous message passing for a :class:`WebSocketProtocol`.
On the server, the communication is started by the
:class:`WebSocket` middleware after a successful handshake.
 
Override :meth:`on_message` to handle incoming string messages,
:meth:`on_bytes` to handle incoming ``bytes`` messages
You can also override :meth:`on_open` and :meth:`on_close` to handle opened
and closed connections.
These methods accept as first parameter the
:class:`WebSocketProtocol` created during the handshake.
'''
    def on_open(self, websocket):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, websocket, message):
        '''Handles incoming messages on the WebSocket.
This method must be overloaded.'''
        pass
    
    def on_bytes(self, websocket, body):
        '''Handles incoming bytes.'''
        pass
    
    def on_ping(self, websocket, body):
        '''Handle incoming ping ``Frame``.'''
        websocket.write(self.pong(websocket))
        
    def on_pong(self, websocket, body):
        '''Handle incoming pong ``Frame``.'''
        pass

    def on_close(self, websocket):
        """Invoked when the WebSocket is closed."""
        pass
    
    def pong(self, websocket, body=None):
        '''Return a ``pong`` frame.'''
        return websocket.parser.pong(body)
    
    def ping(self, websocket, body=None):
        '''Return a ``ping`` frame.'''
        return websocket.parser.ping(body)
        
