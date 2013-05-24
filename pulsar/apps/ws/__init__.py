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


class GeneralWebSocket(wsgi.Router):
    '''A websocket middleware.
    
.. attribute:: frame_parser

    A factory of websocket frame parsers
'''
    namespace = ''
    frame_parser = FrameParser
    
    def __init__(self, route, handle, frame_parser=None, start_response=False,
                 **kwargs):
        super(GeneralWebSocket, self).__init__(route, **kwargs)
        self.handle = handle
        self.start_response = start_response
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
        '''handle the websocket handshake. Must return a list of HTTP
headers to send back to the client.'''
        raise NotImplementedError
        
    
class WebSocket(GeneralWebSocket):
    """A specialised :ref:`Router <wsgi-router>` middleware for
handling the websocket handshake at a given route.
Once the handshake is succesful, the protocol consumer
is upgraded to :class:`WebSocketProtocol` and messages are handled by
the :attr:`handle` attribute, an instance of :class:`WS`.

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
These methods accept as first parameter the
:class:`pulsar.apps.wsgi.wrappers.WsgiRequest` created during the handshake.
'''
    def on_open(self, request):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, request, message):
        '''Handles incoming messages on the WebSocket.
This method must be overloaded. Whatever is returned by this method
is handled by :class:`WebSocketProtocol.write` method.'''
        raise NotImplementedError

    def on_close(self, request):
        """Invoked when the WebSocket is closed."""
        pass
        
    def write(self, request, message):
        '''An utility method for writing a message to the client.
It uses the :class:`WebSocketProtocol` which is accessible from the *request*
parameter. It is a proxy for the :class:`WebSocketProtocol.write` method.'''
        return request.cache['websocket'].write(message)
        
        
class WebSocketProtocol(pulsar.ProtocolConsumer):
    '''Websocket protocol for servers and clients.'''
    request = None
    started = False
    closed = False
    
    def data_received(self, data):
        frame = self.parser.decode(data)
        while frame:
            self.write(self.parser.replay_to(frame))
            if frame.is_close:
                return self.close()
            if not self.started:
                self.started = True
                self.write(self.handler.on_open(self.request))
            if frame.is_data:
                self.write(self.handler.on_message(self.request, frame.body))
            frame = self.parser.decode()
    
    def write(self, value):
        '''Write a new *frame* into the wire, frame can be:

* ``None`` does nothing
* bytes - converted to a byte Frame
* string - converted to a string Frame
* a :class:`pulsar.utils.websocket.Frame`
 '''
        self._async_write(value).add_errback(self.close)
            
    def close(self, error=None):
        if not self.closed:
            log_failure(error)
            self.closed = True
            self.handler.on_close(self.request)
            self.transport.close()
    
    # INTERNAL 
    @async(get_result=False)
    def _async_write(self, value):
        frame = yield value
        if frame is None:
            return
        elif not isinstance(frame, Frame):
            frame = self.parser.encode(frame)
            self.transport.write(frame.msg)
        else:
            self.transport.write(frame.msg)
        if frame.is_close:
            self.close()
    
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
