'''The :mod:`pulsar.apps.ws` contains WSGI_ middleware for
handling the WebSocket_ protocol.
Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`WebSocket` for the handshake_ and a class derived from
:class:`WS` handler for the communication part.

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
from pulsar import is_async, HttpException, ProtocolError, log_failure
from pulsar.utils.httpurl import to_bytes, native_str
from pulsar.utils.websocket import FrameParser, Frame
from pulsar.apps import wsgi

from . import extensions

WEBSOCKET_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


class GeneralWebSocket(wsgi.Router):
    '''A websocket middleware.
    
.. attribute:: frame_parser

    A factory of websocket frame parsers
'''
    namespace = ''
    frame_parser = FrameParser
    
    def __init__(self, route, handle, frame_parser=None, **kwargs):
        super(GeneralWebSocket, self).__init__(route, **kwargs)
        self.handle = handle
        if frame_parser:
            self.frame_parser = frame_parser
        
    def get_client(self):
        return self.handle(self)
    
    def get(self, request):
        headers, parser = self.handle_handshake(request.environ)
        request.response.status_code = 101
        request.response.content = b''
        request.response.headers.update(headers)
        upgrade = request.environ['pulsar.connection'].upgrade
        upgrade(partial(WebSocketServerProtocol, self.handle,
                        request, parser))
        return request.response.start()
    
    def handle_handshake(self, environ):
        '''handle the websocket handshake. Must return a list of HTTP
headers to send back to the client.'''
        raise NotImplementedError
        
    
class WebSocket(GeneralWebSocket):
    """A :class:`pulsar.apps.wsgi.Router` middleware for handling
the websocket handshake at a given route. Once the handshake is succesful,
it upgrades the websocket protocol served by a custom :class:`WS`
handler.

To create a valid :class:`WebSocket` middleware initialise as follow::

    from pulsar.apps import wsgi, ws
    
    class MyWS(ws.WS):
        ...
    
    wm = ws.WebSocket('/bla', MyWS())
    app = wsgi.WsgiHandler(middleware=(..., wm))
    wsgi.WSGIServer(callable=app).start()


See http://tools.ietf.org/html/rfc6455 for the websocket server protocol and
http://www.w3.org/TR/websockets/ for details on the JavaScript interface.
    """        
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
        return headers, parser
        
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
    def on_open(self, request):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, request, message):
        """Handle incoming messages on the WebSocket.
        This method must be overloaded.
        """
        raise NotImplementedError()

    def on_close(self, request):
        """Invoked when the WebSocket is closed."""
        pass
        
        
class WebSocketProtocol(pulsar.ProtocolConsumer):
    '''Websocket protocol for servers and clients.'''
    request = None
    started = False
    closed = False
    
    def data_received(self, data):
        request = self.request
        parser = self.parser
        frame = parser.decode(data)
        while frame:
            self.write(parser.replay_to(frame))
            if not self.started:
                # call on_start (first message received)
                self.started = True
                self.write(self.handler.on_open(request))
            if frame.is_close:
                # Close the connection
                self.close()
            elif frame.is_data:
                self.write(self.handler.on_message(request, frame.body))
            frame = parser.decode()
    
    def write(self, frame):
        '''Write a new *frame* into the wire, frame can be byes, asynchronous
or  nothing. This is a utility method for the transport write method.'''
        if frame is None:
            return
        elif is_async(frame):
            return frame.add_callback(self.write, self.close)
        elif not isinstance(frame, Frame):
            frame = self.parser.encode(frame)
            self.transport.write(frame.msg)
        else:
            self.transport.write(frame.msg)
        if frame.is_close:
            self.close()
            
    def close(self, error=None):
        if not self.closed:
            log_failure(error)
            self.closed = True
            self.handler.on_close(self.environ)
            self.transport.close()
    
    
class WebSocketServerProtocol(WebSocketProtocol):
    '''Created after a successful websocket handshake. Tjis is a
:class:`pulsar.ProtocolConsumer` which manages the communication with the
websocket client.'''
    def __init__(self, handler, request, parser, connection):
        super(WebSocketServerProtocol, self).__init__(connection)
        connection.set_timeout(0)
        self.handler = handler
        self.request = request
        self.parser = parser
        request.cache['websocket'] = self
    
        
        