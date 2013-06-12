'''The :mod:`pulsar.apps.ws` contains WSGI_ middleware for
handling the WebSocket_ protocol.
Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`WebSocket` for the handshake_ and a class derived from
:class:`WS` handler for the communication part.

Here is an example Web Socket handler that echos back all received messages
back to the client::

    from pulsar.apps import wsgi, ws
    
    class EchoWS(ws.WS):
    
        def on_open(self, request):
            pass
    
        def on_message(self, request, message):
            return message
    
        def on_close(self, request):
            pass
            
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
import logging
import socket
import re
import struct
import time
import base64
import hashlib
from functools import partial

import pulsar
from pulsar import async, HttpException, ProtocolError, log_failure
from pulsar.utils.httpurl import to_bytes, native_str
from pulsar.utils.websocket import FrameParser, Frame
from pulsar.apps import wsgi

from . import extensions

WEBSOCKET_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


class WebSocket(wsgi.Router):
    """A specialised :ref:`Router <wsgi-router>` middleware for
handling the websocket handshake at a given route.
Once the handshake is succesful, the protocol consumer
is upgraded to :class:`WebSocketProtocol` and messages are handled by
the :attr:`handle` attribute, an instance of :class:`WS`.

See http://tools.ietf.org/html/rfc6455 for the websocket server protocol and
http://www.w3.org/TR/websockets/ for details on the JavaScript interface.

.. attribute:: frame_parser

    A factory of websocket frame parsers
    """
    frame_parser = FrameParser
    
    def __init__(self, route, handle, frame_parser=None, **kwargs):
        super(WebSocket, self).__init__(route, **kwargs)
        self.handle = handle
        if frame_parser:
            self.frame_parser = frame_parser
            
    def get(self, request):
        headers_parser = self.handle_handshake(request.environ)
        if not headers_parser:
            raise HttpException(status=404)
        headers, parser = headers_parser
        request.response.status_code = 101
        request.response.content = b''
        request.response.headers.update(headers)
        upgrade = request.environ['pulsar.connection'].upgrade
        upgrade(partial(WebSocketServerProtocol, self.handle,
                        request, parser))
        return request.response
    
    def handle_handshake(self, environ):
        connections = environ.get("HTTP_CONNECTION", '').lower()\
                                    .replace(' ','').split(',')
        if environ.get("HTTP_UPGRADE", '').lower() != "websocket" or \
           'upgrade' not in connections:
            return
        if environ['REQUEST_METHOD'].upper() != 'GET':
            raise HttpException(msg='Method is not GET', status=400)
        key = environ.get('HTTP_SEC_WEBSOCKET_KEY')
        if key:
            try:
                ws_key = base64.b64decode(key.encode('latin-1'))
            except Exception:
                ws_key = ''
            if len(ws_key) != 16:
                raise HttpException(msg="WebSocket key's length is invalid",
                                    status=400)
        else:
            raise HttpException(msg='Not a valid HyBi WebSocket request. '
                                    'Missing Sec-Websocket-Key header.',
                                status=400)
        # Collect supported subprotocols
        subprotocols = environ.get('HTTP_SEC_WEBSOCKET_PROTOCOL')
        ws_protocols = []
        if subprotocols:
            for s in subprotocols.split(','):
                ws_protocols.append(s.strip())
        # Collect supported extensions
        ws_extensions = []
        extensions = environ.get('HTTP_SEC_WEBSOCKET_EXTENSIONS')
        if extensions:
            for ext in extensions.split(','):
                ws_extensions.append(ext.strip())
        # Build the frame parser
        version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        try:
            parser = self.frame_parser(version=version, protocols=ws_protocols,
                                       extensions=ws_extensions)
        except ProtocolError as e:
            raise HttpException(str(e), status=400)
        headers = [('Sec-WebSocket-Accept', self.challenge_response(key))]
        if parser.protocols:
            headers.append(('Sec-WebSocket-Protocol',
                            ', '.join(parser.protocols)))
        if parser.extensions:
            headers.append(('Sec-WebSocket-Extensions',
                            ','.join(parser.extensions)))
        return headers, parser
        
    def challenge_response(self, key):
        sha1 = hashlib.sha1(to_bytes(key+WEBSOCKET_GUID))
        return native_str(base64.b64encode(sha1.digest()))


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
    def on_open(self, request):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, request, message):
        '''Handles incoming messages on the WebSocket.
This method must be overloaded.'''
        pass
    
    def on_bytes(self, request, body):
        '''Handles incoming bytes'''
        pass
    
    def on_ping(self, request, body):
        '''Handle incoming ping ``Frame``.'''
        self.write(request, self.pong(request))
        
    def on_pong(self, request, body):
        '''Handle incoming pong ``Frame``.'''
        pass

    def on_close(self, request):
        """Invoked when the WebSocket is closed."""
        pass
        
    def write(self, request, message):
        '''An utility method for writing a message to the client.
It uses the :class:`WebSocketProtocol` which is accessible from the *request*
parameter. It is a proxy for the :class:`WebSocketProtocol.write` method.'''
        return request.cache.websocket.write(message)
    
    def pong(self, request, body=None):
        '''Return a ``pong`` frame.'''
        return request.cache.websocket.parser.pong(body)
    
    def ping(self, request, body=None):
        '''Return a ``ping`` frame.'''
        return request.cache.websocket.parser.ping(body)
        
        
class WebSocketProtocol(pulsar.ProtocolConsumer):
    '''WebSocket protocol for servers and clients.'''
    request = None
    started = False
    closed = False
    
    def data_received(self, data):
        frame = self.parser.decode(data)
        while frame:
            if frame.is_close:
                self.close()
            else:
                self._handle_frame(frame)
            frame = self.parser.decode()
    
    @async()
    def _handle_frame(self, frame):
        if not self.started:
            self.started = True
            yield self.handler.on_open(self.request)
        if frame.is_message:
            yield self.handler.on_message(self.request, frame.body)
        elif frame.is_bytes:
            yield self.handler.on_bytes(self.request, frame.body)
        elif frame.is_ping:
            yield self.handler.on_ping(self.request, frame.body)
        elif frame.is_pong:
            yield self.handler.on_pong(self.request, frame.body)
            
    def write(self, frame):
        '''Write a new ``frame`` into the wire,
``frame`` can be:

* ``None`` does nothing
* ``bytes`` - converted to a byte Frame
* ``string`` - converted to a string Frame
* a :class:`pulsar.utils.websocket.Frame`
 '''
        if not isinstance(frame, Frame):
            frame = self.parser.encode(frame)
        self.transport.write(frame.msg)
        if frame.is_close:
            self.close()
            
    def close(self, error=None):
        if not self.closed:
            log_failure(error)
            self.closed = True
            self.handler.on_close(self.request)
            self.transport.close()

    
class WebSocketServerProtocol(WebSocketProtocol):
    '''The :class:`WebSocketProtocol` for servers, created after a successful
:class:`WebSocket` handshake.'''
    def __init__(self, handler, request, parser, connection):
        super(WebSocketServerProtocol, self).__init__(connection)
        connection.set_timeout(0)
        self.handler = handler
        self.request = request
        self.parser = parser
        request.cache['websocket'] = self
