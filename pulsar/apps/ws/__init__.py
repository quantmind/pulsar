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
from pulsar import is_async, HttpException, ProtocolError
from pulsar.utils.httpurl import to_bytes, native_str
from pulsar.utils.websocket import FrameParser, Frame
from pulsar.apps import wsgi

from . import extensions

WEBSOCKET_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


class GeneralWebSocket(object):
    '''A websocket middleware.
    
.. attribute:: frame_parser

    A factory of websocket frame parsers
'''
    namespace = ''
    frame_parser = FrameParser
    
    def __init__(self, handle, frame_parser=None):
        self.handle = handle
        if frame_parser:
            self.frame_parser = frame_parser

    def get_client(self):
        return self.handle(self)
    
    def __call__(self, environ, start_response):
        if self.handle.match(environ):
            headers, parser = self.handle_handshake(environ, start_response)
            response = wsgi.WsgiResponse(101, (b'',), response_headers=headers)
            upgrade = environ['connection.upgrade']
            upgrade(partial(WebSocketProtocol, self.handle, environ, parser))
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
        version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        if version:
            try:
                version = int(version)
            except Exception:
                pass
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
        try:
            parser = self.frame_parser(version=version, protocols=ws_protocols,
                                       extensions=ws_extensions)
        except ProtocolError as e:
            raise HttpException(str(e), status=400)
        headers = [
            ('Upgrade', 'websocket'),
            ('Connection', 'Upgrade'),
            ('Sec-WebSocket-Accept', self.challenge_response(key))
        ]
        if parser.protocols:
            headers.append(('Sec-WebSocket-Protocol',
                            ', '.join(parser.protocols)))
        if parser.extensions:
            headers.append(('Sec-WebSocket-Extensions',
                            ','.join(parser.extensions)))
        return self.handle.on_handshake(environ, headers), parser
        
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
    
    def __init__(self, handler, environ, parser, connection):
        super(WebSocketProtocol, self).__init__(connection)
        self.handler = handler
        self.environ = environ
        self.parser = parser
        self.started = False
        self.closed = False
        
    def data_received(self, data):
        environ = self.environ
        parser = self.parser
        frame = parser.decode(data)
        while frame:
            self.write(parser.replay_to(frame))
            if not self.started:
                # call on_start (first message received)
                self.started = True
                self.write(self.handler.on_open(environ))
            if frame.is_close:
                # Close the connection
                self.close()
            elif frame.is_data:
                self.write(self.handler.on_message(environ, frame.body))
            frame = parser.decode()
    
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
    
    
class WebSocketClientProtocol(pulsar.ProtocolConsumer):
    
    def __init__(self, *args):
        super(WebSocketClientProtocol, self).__init__(*args)
        self.parser = FrameParser(kind=1)
        
    def data_received(self, data):
        frame = self.parser.decode(data)
        while frame:
            # Got a frame
            self.consumer(frame)
            frame = self.parser.decode()
        # No more frames
        
            

class HttpClient(wsgi.HttpClient):
    
    def upgrade(self, response):
        client = WebSocketClient(response.sock, response.url)
        client.handshake = response
        return client
        