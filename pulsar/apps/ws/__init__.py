'''The :mod:`pulsar.apps.ws` contains a WSGI middleware for
handling the WebSocket_ protocol.

Web sockets allow for bidirectional communication between the browser
and server. Pulsar implementation uses the WSGI middleware
:class:`WebSocket` for the handshake and a class derived from
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
import logging
import socket
import re
import struct
import time
import base64
import hashlib
from functools import partial

import pulsar
from pulsar import safe_async
from pulsar.utils.httpurl import ispy3k, to_bytes, native_str,\
                                 itervalues, parse_qs
from pulsar.apps.wsgi import WsgiResponse, wsgi_iterator

from .frame import *


__all__ = ['WebSocket', 'SocketIOMiddleware']


class GeneralWebSocket(object):
    namespace = ''
    extensions = ['x-webkit-deflate-frame']
    
    def __init__(self, handle, namespace=None, clients=None, extensions=None):
        self.handle = handle
        if extensions is None:
            extensions = self.extensions
        self.extensions = extensions
        self._clients = clients if clients is not None else {}
        self.namespace = namespace if namespace is not None else self.namespace
    
    @property
    def clients(self):
        return frozenset(itervalues(self._clients))

    def get_client(self):
        return self.handle(self)
    
    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO')
        if not path.lstrip('/').startswith(self.namespace):
            return
        return self.handle_handshake(environ, start_response)
    
    def handle_handshake(self, environ, start_response, client = None):
        raise NotImplementedError()
    
    def get_parser(self):
        raise NotImplementedError()
            
    
class WebSocket(GeneralWebSocket):
    """A :ref:`WSGI <apps-wsgi>` middleware for serving web socket applications.
It implements the protocol version 8 as specified at
http://www.whatwg.org/specs/web-socket-protocol/.

Web Sockets are not standard HTTP connections. The "handshake" is HTTP,
but after that, the protocol is message-based. To create
a valid :class:`WebSocket` instance initialise as follow::

    from pulsar.apps import wsgi, ws
    
    class MyWebSocket(ws.WS):
        ...
    
    wm = ws.WebSocket(handle = MyWebSocket())
        
    app = wsgi.WsgiHandler(middleware = (...,wm))
    
    wsgi.createServer(callable = app).start()


See http://tools.ietf.org/html/rfc6455 for the websocket server protocol and
http://www.w3.org/TR/websockets/ for details on the JavaScript interface.
    """
    VERSIONS = ('8','13')
    WS_KEY = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
    #magic string
    STATUS = "101 Switching Protocols"
    
    def handle_handshake(self, environ, start_response):
        connections = environ.get("HTTP_CONNECTION", '').lower()\
                                    .replace(' ','').split(',')
        if environ.get("HTTP_UPGRADE", '').lower() != "websocket" or \
           'upgrade' not in connections:
            return
        
        if environ['REQUEST_METHOD'].upper() != 'GET':
            raise WebSocketError(400, reason='Method is not GET')
        
        key = environ.get('HTTP_SEC_WEBSOCKET_KEY')
        if key:
            ws_key = base64.b64decode(key.encode('latin-1'))
            if len(ws_key) != 16:
                raise WebSocketError(400, "WebSocket key's length is invalid")
        else:
            raise WebSocketError(400, 'Not a valid HyBi WebSocket request. '
                                      'Missing Sec-Websocket-Key header.')
        
        version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        if version not in self.VERSIONS:
            raise WebSocketError('Unsupported WebSocket version {0}'\
                                 .format(version))
        
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
        
        start_response(self.STATUS, headers)
        return self.handle(self, environ, version, ws_protocols, ws_extensions)
        
    def challenge_response(self, key):
        sha1 = hashlib.sha1(to_bytes(key+self.WS_KEY))
        return native_str(base64.b64encode(sha1.digest()))
    
    def get_parser(self):
        return FrameParser()


class SocketIOMiddleware(GeneralWebSocket):
    '''A WSGI middleware for socket.io_ client.
    
.. _socket.io: https://github.com/LearnBoost/socket.io-client
'''
    namespace = 'socket.io'
    RE_REQUEST_URL = re.compile(r"""
        ^/(?P<namespace>[^/]+)
         /(?P<protocol_version>[^/]+)
         /(?P<transport_id>[^/]+)
         /(?P<session_id>[^/]+)/?$
         """, re.X)
    RE_HANDSHAKE_URL = re.compile(r"^/(?P<namespace>[^/]+)/1/$", re.X)
    
    handler_types = {
        'websocket': WebSocket
    }
    
    def __init__(self, handle, namespace=None, extensions=None):
        super(SocketIOMiddleware, self).__init__(handle,namespace=namespace,
                                                 extensions=extensions)
        ht = self.handler_types
        self._middlewares = dict(((k,ht[k](handle,\
                                    clients = self._clients)) for k in ht))
    
    def handle_handshake(self, environ, start_response):
        path = environ.get('PATH_INFO')
        request_method = environ.get("REQUEST_METHOD")
        request_tokens = self.RE_REQUEST_URL.match(path)
        
        # Parse request URL and QUERY_STRING and do handshake
        if request_tokens:
            request_tokens = request_tokens.groupdict()
        else:
            handshake_tokens = self.RE_HANDSHAKE_URL.match(path)
            if handshake_tokens:
                return self._io_handshake(environ, start_response,
                                          handshake_tokens.groupdict())
            else:
                return

        # Delegate to transport protocol
        transport = self._middlewares.get(request_tokens["transport_id"])
        return transport.handle_handshake(environ, start_response)
        
    ############################################################################
    ##    Private
    ############################################################################
    
    def _io_handshake(self, environ, start_response, tokens):
        if tokens["namespace"] != self.namespace:
            raise WebSocketError(400, "Namespace mismatch")
        else:
            client = self.get_client()
            self._clients.pop(client.id)
            data = "%s:15:10:%s" % (client.id, ",".join(self._middlewares))
            args = parse_qs(environ.get("QUERY_STRING"))
            if "jsonp" in args:
                content_type = 'application/javascript'
                data = 'io.j[%s]("%s");' % (args["jsonp"][0], data)
            else:
                content_type = 'text/plain'
            return WsgiResponse(200,
                                data.encode('utf-8'),
                                content_type = content_type)
    
    
class WS(object):
    '''A web socket live connection. An instance of this calss maintain
and open socket with a remote web-socket and exchange messages in
an asynchronous fashion. A :class:`WS` is initialized by :class:`WebSocket`
middleware at every new web-socket connection.
 
Override :meth:`on_message` to handle incoming messages.
You can also override :meth:`on_open` and :meth:`on_close` to handle opened
and closed connections.

Here is an example Web Socket handler that echos back all received messages
back to the client::

    class EchoWebSocket(websocket.WebSocketHandler):
        def on_open(self):
            print "WebSocket opened"
    
        def on_message(self, message):
            self.write_message(u"You said: " + message)
    
        def on_close(self):
            print "WebSocket closed"
            
If you map the handler above to "/websocket" in your application, you can
invoke it in JavaScript with::

    var ws = new WebSocket("ws://localhost:8888/websocket");
    ws.onopen = function() {
       ws.send("Hello, world");
    };
    ws.onmessage = function (evt) {
       alert(evt.data);
    };

This script pops up an alert box that says "You said: Hello, world".

.. attribute: protocols

    list of protocols from the handshake
    
.. attribute: extensions

    list of extensions from the handshake
'''
    def __init__(self, middleware, environ, version, protocols, extensions):
        self.middleware = middleware
        self.environ = environ
        self.version = version
        self.protocols = protocols
        self.extensions = extensions
        self.connection = self.environ['pulsar.connection']
    
    def __repr__(self):
        return '%s %s (id=%s)' % (self.__class__.__name__, self.path)
    __str__ = __repr__
        
    @property
    def path(self):
        return self.environ.get('PATH_INFO','')
    
    @property
    def started(self):
        return hasattr(self, 'environ')
    
    def __iter__(self):
        return wsgi_iterator(self._generator())
        
    @property
    def client_terminated(self):
        return self.stream.closed
        
    def on_open(self):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, message):
        """Handle incoming messages on the WebSocket.
        This method must be overloaded.
        """
        raise NotImplementedError()

    def on_close(self):
        """Invoked when the WebSocket is closed."""
        pass
    
    def write_message(self, message, binary=False):
        """Sends the given message to the client of this Web Socket."""
        self.out_frames.append(frame(version=self.version,
                                     message=to_bytes(message),
                                     binary=binary))
    
    def close(self):
        """Closes the WebSocket connection."""
        msg = frame_close(version=self.version)
        self.stream.write(frame).add_callback(self._abort)
        self._started_closing_handshake = True
        
    #################################################################    
    # INTERNALS
    #################################################################
    def _generator(self):
        socket = self.connection.socket
        yield self.on_open() or b''
        while not socket.closed:
            d = socket.read()
            yield d
            frame = self.parser.execute(d.result)
            if frame and frame.is_complete():
                yield frame.on_complete(self)

