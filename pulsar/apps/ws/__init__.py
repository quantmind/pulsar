'''The :mod:`pulsar.apps.ws` contains a WSGI compatible implementation
of the WebSocket_ protocol.

Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`WebSocket` for the handshake and a class derived from
:class:`WS` for the communication part. 

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