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
from pulsar import maybe_async, is_async, safe_async, is_failure, ClientSocket
from pulsar.utils.httpurl import ispy3k, to_bytes, native_str,\
                                 itervalues, parse_qs, WEBSOCKET_VERSION
from pulsar.apps.wsgi import WsgiResponse, wsgi_iterator

from .frame import *

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
            return self.handle_handshake(environ, start_response)
    
    def handle_handshake(self, environ, start_response):
        raise NotImplementedError()
    
    def upgrade_connection(self, environ, version):
        '''Upgrade the connection so it handles the websocket protocol.'''
        connection = environ['pulsar.connection']
        connection.environ = environ
        #connection.read_timeout = 0
        connection.protocol = FrameParser(version)
        connection.response_class = self.on_message
        
    def on_message(self, connection, frame):
        environ = connection.environ
        if not environ.get('websocket-opened'):
            environ['websocket-opened'] = True
            self.handle.on_open(environ)
        rframe = frame.on_received()
        if rframe:
            yield rframe.msg
        elif frame.is_close:
            # Close
            connection.close()
        elif frame.is_data:
            yield self.as_frame(connection,
                                self.handle.on_message(environ, frame.body))

    def as_frame(self, connection, body):
        '''Build a websocket server frame from body.'''
        body = maybe_async(body)
        if is_async(body):
            return body.addBoth(lambda b: self.as_frame(connection, b))
        if is_failure(body):
            # We have a failure. shut down connection
            body.log()
            body = Frame.close('Server error')
        elif not isinstance(body, Frame):
            # If the body is not a frame, build a final frame from it.
            body = Frame(body or '', version=connection.protocol.version,
                         final=True)
        return body.msg
        
    
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
            except:
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
            except:
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
        self.handle.on_handshake(environ, headers)
        self.upgrade_connection(environ, version)
        response = WsgiResponse(101, content=(), response_headers=headers)
        return response(environ, start_response)
        
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
        pass
    
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
    
    def close(self, environ, msg=None):
        '''Invoked when the web-socket needs closing.'''
        connection = environ['pulsar.connection']
        return Frame.close(msg, version=connection.protocol.version)
    
    
class WebSocketClient(ClientSocket):
    
    def isclosed(self):
        # For compatibility with HttpResponse
        return self.closed
    
    def protocol_factory(self):
        return FrameParser(kind=1)
    

class HttpClient(pulsar.HttpClient):
    
    def upgrade(self, response):
        client = WebSocketClient(response.sock, response.url)
        client.handshake = response
        return client
        