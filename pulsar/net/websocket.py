# Author: Jacob Kristhammar, 2010
#
# Updated version of websocket.py[1] that implements latest[2] stable version
# of the websocket protocol.
#
# NB. It's no longer possible to manually select which callback that should
#     be invoked upon message reception. Instead you must override the
#     on_message(message) method to handle incoming messsages.
#     This also means that you don't have to explicitly invoke
#     receive_message, in fact you shouldn't.
#
# [1] http://github.com/facebook/tornado/blob/
#     2c89b89536bbfa081745336bb5ab5465c448cb8a/tornado/websocket.py
# [2] http://tools.ietf.org/html/draft-hixie-thewebsocketprotocol-76
import hashlib
import logging
import socket
import re
import struct
import time
import base64
from hashlib import sha1
from functools import partial

import pulsar
from pulsar.utils.py2py3 import ispy3k, to_bytestring, BytesIO


__all__ = ['WebSocket','WS']


class WebSocketError(pulsar.BadHttpRequest):
    pass        
        

class WS(object):
    '''Override on_message to handle incoming messages. You can also override
open and on_close to handle opened and closed connections.
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
        self.protocols = protocols
        self.extensions = extensions
        self.stream = environ['wsgi.input']
        
    def __iter__(self):
        #yield en empty string so that headers are sent
        yield ''
        stream = self.stream
        self.on_open()
        stream.read(self._on_frame_start,2)
        
    def on_open(self):
        """Invoked when a new WebSocket is opened."""
        pass

    def on_message(self, message):
        """Handle incoming messages on the WebSocket

        This method must be overloaded
        """
        raise NotImplementedError

    def on_close(self):
        """Invoked when the WebSocket is closed."""
        pass
    
    def write_message(self, message, binary=False):
        """Sends the given message to the client of this Web Socket."""
        message = to_bytestring(message)
        if not binary:
            opcode = 0x1
        else:
            opcode = 0x2
        return self._write_frame(True, opcode, message)
        
    def _write_frame(self, fin, opcode, data):
        if fin:
            finbit = 0x80
        else:
            finbit = 0
        frame = struct.pack("B", finbit | opcode)
        l = len(data)
        if l < 126:
            frame += struct.pack("B", l)
        elif l <= 0xFFFF:
            frame += struct.pack("!BH", 126, l)
        else:
            frame += struct.pack("!BQ", 127, l)
        frame += data
        return self.stream.write(frame)

    def _on_frame_start(self, data):
        header, payloadlen = struct.unpack("BB", data)
        self._final_frame = header & 0x80
        self._frame_opcode = header & 0xf
        if not (payloadlen & 0x80):
            # Unmasked frame -> abort connection
            self._abort()
        payloadlen = payloadlen & 0x7f
        if payloadlen < 126:
            self._frame_length = payloadlen
            self.stream.read_bytes(self._on_masking_key)
        elif payloadlen == 126:
            self.stream.read_bytes(self._on_frame_length_16)
        elif payloadlen == 127:
            self.stream.read_bytes(self._on_frame_length_64)

    def _on_frame_length_16(self, data):
        self._frame_length = struct.unpack("!H", data)[0];
        self.stream.read_bytes(4, self._on_masking_key);
        
    def _on_frame_length_64(self, data):
        self._frame_length = struct.unpack("!Q", data)[0];
        self.stream.read_bytes(4, self._on_masking_key);

    def _on_masking_key(self, data):
        self._frame_mask = bytearray(data)
        self.stream.read_bytes(self._frame_length, self._on_frame_data)

    def _on_frame_data(self, data):
        unmasked = bytearray(data)
        for i in xrange(len(data)):
            unmasked[i] = unmasked[i] ^ self._frame_mask[i % 4]

        if not self._final_frame:
            if self._fragmented_message_buffer:
                self._fragmented_message_buffer += unmasked
            else:
                self._fragmented_message_opcode = self._frame_opcode
                self._fragmented_message_buffer = unmasked
        else:
            if self._frame_opcode == 0:
                unmasked = self._fragmented_message_buffer + unmasked
                opcode = self._fragmented_message_opcode
                self._fragmented_message_buffer = None
            else:
                opcode = self._frame_opcode

            self._handle_message(opcode, bytes_type(unmasked))

        if not self.client_terminated:
            self._receive_frame()
        

    def _handle_message(self, opcode, data):
        if self.client_terminated: return
        
        if opcode == 0x1:
            # UTF-8 data
            self.async_callback(self.handler.on_message)(data.decode("utf-8", "replace"))
        elif opcode == 0x2:
            # Binary data
            self.async_callback(self.handler.on_message)(data)
        elif opcode == 0x8:
            # Close
            self.client_terminated = True
            if not self._started_closing_handshake:
                self._write_frame(True, 0x8, b(""))
            self.stream.close()
        elif opcode == 0x9:
            # Ping
            self._write_frame(True, 0xA, data)
        elif opcode == 0xA:
            # Pong
            pass
        else:
            self._abort()


class WebSocket(object):
    """A WSGI_ middleware for serving web socket applications.
It implements the protocol version 8 as specified at
http://www.whatwg.org/specs/web-socket-protocol/.

Web Sockets are not standard HTTP connections. The "handshake" is HTTP,
but after the handshake, the protocol is message-based. To create
a valid :class:`WebSocket` instance initialize as follow::

    ws = MyWebSocket()
    w = WebSocket(handler = ws)

where ``ws`` is an implementation of :class:`WS`.

The communication methods available to you are write_message()
and close(). Likewise, your request handler class should
implement open() method rather than get() or post().


See http://www.w3.org/TR/websockets/ for details on the
JavaScript interface.
    """
    VERSIONS = ('8',)
    WS_KEY = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
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
        key = key + self.WS_KEY
        key = key.encode('utf8')
        headers = [
            ('Upgrade', 'websocket'),
            ('Connection', 'Upgrade'),
            ('Sec-WebSocket-Accept', base64.b64encode(sha1(key).digest()))
        ]
        if ws_protocols:
            headers.append(('Sec-WebSocket-Protocol',
                            ', '.join(ws_protocols)))
        if ws_extensions:
            headers.append(('Sec-WebSocket-Extensions',
                            ','.join(ws_extensions)))
        
        start_response(self.STATUS, headers)
        return self.handle(environ,ws_protocols,ws_extensions)
        
