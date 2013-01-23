'''The :mod:`pulsar.apps.ws` contains a WSGI_ middleware for
handling the WebSocket_ protocol.
Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`WebSocket` for the handshake and a class derived from
:class:`WS` handler for the communication part.

.. _WSGI: http://www.python.org/dev/peps/pep-3333/
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

Frame
~~~~~~~~~~~~~~~~~~~

.. autoclass:: Frame
   :members:
   :member-order: bysource
   
   
.. autoclass:: FrameParser
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
from pulsar import maybe_async, is_async, safe_async, is_failure
from pulsar.utils.httpurl import ispy3k, to_bytes, native_str,\
                                 itervalues, parse_qs, WEBSOCKET_VERSION
from pulsar.apps.wsgi import WsgiResponse, wsgi_iterator
from pulsar.utils.websocket import FrameParser, Frame

WEBSOCKET_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


class GeneralWebSocket(object):
    namespace = ''
    extensions = ['x-webkit-deflate-frame']
    
    def __init__(self, handle, extensions=None):
        self.handle = handle
        if extensions is None:
            extensions = self.extensions
        self.extensions = extensions

    def get_client(self):
        return self.handle(self)
    
    def __call__(self, environ, start_response):
        if self.handle.match(environ):
            headers = self.handle_handshake(environ, start_response)
            response = WsgiResponse(101, (b'',), response_headers=headers)
            upgrade = environ['upgrade_protocol']
            upgrade(partial(WebSocketProtocol, self.handle, environ))
            return response(environ, start_response)
    
    def handle_handshake(self, environ, start_response):
        '''handle the websocket handshake. Must return a list of HTTP
headers to send back to the client.'''
        raise NotImplementedError
        
    
class WebSocket(GeneralWebSocket):
    """A :ref:`WSGI <apps-wsgi>` middleware for handling w websocket handshake
and starting a custom :class:`WS` connection.
It implements the protocol version 13 as specified at
http://www.whatwg.org/specs/web-socket-protocol/.

Web Sockets are not standard HTTP connections. The "handshake" is HTTP,
but after that, the protocol is message-based. To create
a valid :class:`WebSocket` middleware initialise as follow::

    from pulsar.apps import wsgi, ws
    
    class MyWS(ws.WS):
        ...
    
    wm = ws.WebSocket(handle=MyWS())
    app = wsgi.WsgiHandler(middleware=(..., wm))
    
    wsgi.createServer(callable=app).start()


See http://tools.ietf.org/html/rfc6455 for the websocket server protocol and
http://www.w3.org/TR/websockets/ for details on the JavaScript interface.
    """        
    def handle_handshake(self, environ, start_response):
        connections = environ.get("HTTP_CONNECTION", '').lower()\
                                    .replace(' ','').split(',')
        if environ.get("HTTP_UPGRADE", '').lower() != "websocket" or \
           'upgrade' not in connections:
            return
        if environ['REQUEST_METHOD'].upper() != 'GET':
            raise WebSocketError('Method is not GET', status=400)
        key = environ.get('HTTP_SEC_WEBSOCKET_KEY')
        if key:
            try:
                ws_key = base64.b64decode(key.encode('latin-1'))
            except Exception:
                ws_key = ''
            if len(ws_key) != 16:
                raise WebSocketError("WebSocket key's length is invalid",
                                     status=400)
        else:
            raise WebSocketError('Not a valid HyBi WebSocket request. '
                                 'Missing Sec-Websocket-Key header.',
                                 status=400)
        version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        if version:
            try:
                version = int(version)
            except Exception:
                pass
        if version not in WEBSOCKET_VERSION:
            raise WebSocketError('Unsupported WebSocket version {0}'\
                                 .format(version),
                                 status=400)
        # Collect supported subprotocols
        subprotocols = environ.get('HTTP_SEC_WEBSOCKET_PROTOCOL')
        ws_protocols = []
        if subprotocols:
            for s in subprotocols.split(','):
                s = s.strip()
                if s in protocols:
                    ws_protocols.append(s)
        # Collect supported extensions
        ws_extensions = []
        extensions = environ.get('HTTP_SEC_WEBSOCKET_EXTENSIONS')
        if extensions:
            exts = self.extensions
            for ext in extensions.split(','):
                ext = ext.strip()
                if ext in exts:
                    ws_extensions.append(ext)
        # Build and start the HTTP response
        headers = [
            ('Upgrade', 'websocket'),
            ('Connection', 'Upgrade'),
            ('Sec-WebSocket-Accept', self.challenge_response(key))
        ]
        if ws_protocols:
            headers.append(('Sec-WebSocket-Protocol',
                            ', '.join(ws_protocols)))
        if ws_extensions:
            headers.append(('Sec-WebSocket-Extensions',
                            ','.join(ws_extensions)))
        return self.handle.on_handshake(environ, headers)
        
    def challenge_response(self, key):
        sha1 = hashlib.sha1(to_bytes(key+WEBSOCKET_GUID))
        return native_str(base64.b64encode(sha1.digest()))
    
    def get_parser(self):
        return FrameParser()


class WS(object):
    '''A web socket handler. It maintains
an open socket with a remote web-socket and exchange messages in
an asynchronous fashion. The communication is started by the
:class:`WebSocket` middleware after a successful handshake.
 
Override :meth:`on_message` to handle incoming messages.
You can also override :meth:`on_open` and :meth:`on_close` to handle opened
and closed connections.

Here is an example Web Socket handler that echos back all received messages
back to the client::

    from pulsar.apps import ws
    
    class EchoWS(ws.WS):
    
        def on_open(self, environ):
            print("WebSocket opened")
    
        def on_message(self, environ, message):
            return message
    
        def on_close(self, environ):
            print("WebSocket closed")
            
'''
    def match(self, environ):
        pass
    
    def on_handshake(self, environ, headers):
        """Invoked just before sending the upgraded **headers** to the
client. This is a chance to add or remove header's entries."""
        return headers
    
    def on_open(self, environ):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, environ, message):
        """Handle incoming messages on the WebSocket.
        This method must be overloaded.
        """
        raise NotImplementedError()

    def on_close(self, environ):
        """Invoked when the WebSocket is closed."""
        pass
    
    
class WebSocketProtocol(pulsar.ProtocolConsumer):
    
    def __init__(self, handler, environ, connection):
        super(WebSocketProtocol, self).__init__(connection)
        self.handler = handler
        self.environ = environ
        self.parser = FrameParser()
        self.started = False
        self.closed = False
        
    def feed(self, data):
        environ = self.environ
        frame = self.parser.decode(data)
        if frame:
            self.write(frame.on_received())
            if not self.started:
                # call on_start (first message received)
                self.started = True
                self.write(self.handler.on_open(environ))
            if frame.is_close:
                # Close the connection
                self.close()
            elif frame.is_data:
                self.write(self.handler.on_message(environ, frame.body))
    
    def write(self, frame):
        if frame is None:
            return
        elif is_async(frame):
            return result.add_callback(self.write, self.close)
        elif not isinstance(frame, Frame):
            frame = self.parser.encode(frame)
            self.transport.write(frame.msg)
        else:
            self.transport.write(frame.msg)
        if frame.is_close:
            self.close()
            
    def close(self, error=None):
        if not self.closed:
            self.closed = True
            self.handler.on_close(self.environ)
            self.transport.close()
    
    
class WebSocketClientProtocol(pulsar.ClientProtocolConsumer):
    
    def __init__(self, *args):
        super(WebSocketClientProtocol, self).__init__(*args)
        self.parser = FrameParser(kind=1)
        
    def feed(self, data):
        frame = self.parser.decode(data)
        while frame:
            # Got a frame
            self.consumer(frame)
            frame = self.parser.decode()
        # No more frames
        
            

class HttpClient(pulsar.HttpClient):
    
    def upgrade(self, response):
        client = WebSocketClient(response.sock, response.url)
        client.handshake = response
        return client
        