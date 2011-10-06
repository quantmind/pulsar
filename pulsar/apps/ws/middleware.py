import hashlib
import logging
import socket
import re
import struct
import time
import base64
import hashlib
from functools import partial

import pulsar
from pulsar.utils.py2py3 import ispy3k, to_bytestring, native_str, BytesIO

from .frame import *


__all__ = ['WebSocket','WS']


logger = logging.getLogger('websocket')


def safe(self, func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception:
        logger.error("Uncaught exception in {0[PATH_INFO]}"\
                        .format(self.environ), exc_info=True)
        self._abort()


class WS(object):
    '''Override :meth:`on_message` to handle incoming messages.
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
    def __init__(self, environ, protocols, extensions):
        self.environ = environ
        self.version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        self.protocols = protocols
        self.extensions = extensions
        self.parser = Parser()
        self.stream = environ['pulsar.stream']
        
    def __iter__(self):
        #yield en empty string so that headers are sent
        yield b''
        stream = self.stream
        self.on_open()
        # kick of reading
        self._handle()
        
    @property
    def client_terminated(self):
        return self.stream.closed()
        
    def on_open(self):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, message):
        """Handle incoming messages on the WebSocket.
        This method must be overloaded
        """
        raise NotImplementedError

    def on_close(self):
        """Invoked when the WebSocket is closed."""
        pass
    
    def write_message(self, message, binary=False):
        """Sends the given message to the client of this Web Socket."""
        message = to_bytestring(message)
        frame = Frame(version = self.version, message = message,
                      binary = binary)
        self.stream.write(frame)
    
    def close(self):
        """Closes the WebSocket connection."""
        frame = Frame(version = self.version, close = True)
        self.stream.write(frame).add_callback(self._abort)
        self._started_closing_handshake = True
        
    def abort(self, r = None):
        self.stream.close()
        
    #################################################################    
    # INTERNALS
    #################################################################
        
    def _write_message(self, msg):
        self.stream.write(msg)
    
    def _handle(self, data = None):
        if data is not None:
            safe(self, self.parser.execute, data, len(data))
                
        frame = self.parser.frame
        if frame.is_complete():
            if self.client_terminated:
                return
            safe(self, frame.on_complete, self)
            
        if not self.client_terminated:
            self.stream.read(callback = self._handle)
            
    

class WebSocket(object):
    """A :ref:`WSGI <apps-wsgi>` middleware for serving web socket applications.
It implements the protocol version 8 as specified at
http://www.whatwg.org/specs/web-socket-protocol/.

Web Sockets are not standard HTTP connections. The "handshake" is HTTP,
but after that, the protocol is message-based. To create
a valid :class:`WebSocket` instance initialise as follow::

    from pulsar.apps import wsgi, ws
    
    class MyWebSocket(ws.WS):
        ...
    
    wm = ws.WebSocket(handler = MyWebSocket())
        
    app = wsgi.WsgiHandler(middleware = (...,wm))
    
    wsgi.createServer(callable = app).start()


See http://www.w3.org/TR/websockets/ for details on the
JavaScript interface.
    """
    VERSIONS = ('8',)
    WS_KEY = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
    #magic string
    STATUS = "101 Switching Protocols"
    
    def __init__(self, handle):
        self.handle = handle
    
    def __call__(self, environ, start_response):
        if environ.get("HTTP_UPGRADE", '').lower() != "websocket" or \
           environ.get("HTTP_CONNECTION", '').lower() != "upgrade":
            return
        
        if environ['REQUEST_METHOD'].upper() != 'GET':
            raise WebSocketError(400,reason = 'Method is not GET')
        
        key = environ.get('HTTP_SEC_WEBSOCKET_KEY')
        if key:
            ws_key = base64.b64decode(key.encode('latin-1'))
            if len(ws_key) != 16:
                raise WebSocketError(400,"WebSocket key's length is invalid")
        else:
            raise WebSocketError(400,"Not a valid HyBi WebSocket request")
        
        version = environ.get('HTTP_SEC_WEBSOCKET_VERSION')
        if version not in self.VERSIONS:
            raise WebSocketError('Unsupported WebSocket version')
        
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
        return self.handle(environ,ws_protocols,ws_extensions)
        
    def challenge_response(self, key):
        sha1 = hashlib.sha1(to_bytestring(key+self.WS_KEY))
        return native_str(base64.b64encode(sha1.digest()))
