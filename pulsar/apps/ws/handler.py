import logging
from uuid import uuid4
from collections import deque

from pulsar import to_bytes

from .frame import *

__all__ = ['WS']


LOGGER = logging.getLogger('websocket')
 

def safe(self, func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        LOGGER.critical("Uncaught exception in %s" % self.path, exc_info=True)
        self.abort()


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
    def __init__(self, middleware):
        self.middleware = middleware
        self.id = self._create_id()
        
    def start(self, environ, stream, version, protocols, extensions):
        if self.started:
            raise RuntimeError('Web socket handler already started')
        self.environ = environ
        self.version = version
        self.protocols = protocols
        self.extensions = extensions
        self.parser = self.middleware.get_parser()
        self.stream = stream
        self.in_frames = deque()
        self.out_frames = deque()
        return self
    
    def __repr__(self):
        return '%s %s (id=%s)' % (self.__class__.__name__, self.path, self.id)
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def path(self):
        return self.environ.get('PATH_INFO','')
    
    @property
    def started(self):
        return hasattr(self, 'environ')
    
    def __iter__(self):
        #yield an empty string so that headers are sent
        yield self.on_open() or b''
        for data in self._generator():
            yield data
        self.abort()
        
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
        
    def abort(self, r = None):
        self.middleware._clients.pop(self.id, None)
        
    #################################################################    
    # INTERNALS
    #################################################################
    
    def _handle(self, data=None):
        frame = safe(self, self.parser.execute, data)
        if frame and frame.is_complete():
            self.in_frames.append(frame)
        d = self.stream.read()
        if d:
            d.add_callback(self._handle)
    
    def _generator(self):
        # start the reading
        self._handle()
        in_frames = self.in_frames
        while not self.client_terminated:
            if in_frames:
                frame = in_frames.popleft()
                message = safe(self, frame.on_complete, self)
                if message:
                    self.write_message(message)
            if self.out_frames:
                yield self.out_frames.popleft()
            else:
                yield b''
            
    def _create_id(self):
        while True:
            id = str(uuid4())[:8]
            if not self._exists_id(id):
                return id
        
    def _exists_id(self, id):
        return id in self.middleware._clients
        
    def _write_message(self, msg):
        self.stream.write(msg)
        