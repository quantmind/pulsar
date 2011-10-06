'''Pulsar comes with a server-side implementation of the WebSocket protocol.
WebSockets_ allow for bidirectional communication between the browser
and server.

The WebSocket protocol is still in development.  This module currently
implements the hybi_-17 version of the protocol.  
See this `browser compatibility table 
<http://en.wikipedia.org/wiki/WebSockets#Browser_support>`_ on Wikipedia.
   
The implementation uses the WSGI middleware
:class:`pulsar.apps.ws.WebSocket` which implements the handshake and the
:class:`pulsar.apps.ws.WS` handler for the communication part. 

.. _WebSockets: http://dev.w3.org/html5/websockets/
.. _hybi: http://tools.ietf.org/html/draft-ietf-hybi-thewebsocketprotocol-17
'''
from .middleware import *