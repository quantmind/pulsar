'''The :mod:`pulsar.apps.ws` contains a WSGI compatible implementation
of the WebSocket_ protocol.

WebSockets allow for bidirectional communication between the browser
and server..The implementation uses the WSGI middleware
:class:`WebSocket` which implements the handshake and the
:class:`WS` handler for the communication part. 

.. _WebSocket: http://tools.ietf.org/html/rfc6455

API
==============

WebSocket
~~~~~~~~~~~~~~~~

.. autoclass:: WebSocket
   :members:
   :member-order: bysource


WebSocket Handler
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: WS
   :members:
   :member-order: bysource

Framing
~~~~~~~~~~~~~~~~~~~

.. autofunction:: frame_close


'''
from .frame import *
from .handler import *
from .middleware import *