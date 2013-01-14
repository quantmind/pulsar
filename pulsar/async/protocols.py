from inspect import isgenerator

from .defer import Deferred

__all__ = ['Protocol', 'ProtocolResponse', 'ChunkResponse', 'ProtocolError']

    
class ProtocolError(Exception):
    '''Raised when the protocol encounter unexpected data. It will close
the socket connection.'''


class ProtocolResponse(object):
    '''A :class:`Protocol` response is responsible for parsing incoming data
and producing no more than one response.'''
    def __init__(self, protocol):
        self._protocol = protocol
        self._finished = False
            
    @property
    def event_loop(self):
        return self._protocol.event_loop
    
    @property
    def sock(self):
        return self._protocol.sock
    
    @property
    def protocol(self):
        return self._protocol
    
    @property
    def transport(self):
        return self._protocol.transport
    
    def on_connect(self):
        pass
    
    def begin(self):
        raise NotImplementedError
        
    def feed(self, data):
        '''Feed new data into the this :class:`ProtocolResponse`. This method
should return `None` unless it has finished the response and the returned bytes
can be used for the next response.'''
        raise NotImplementedError
    
    def finished(self):
        '''`True` if this response has finished and a new response can start.'''
        return self._finished
    
    ############################################################################
    ###    TRANSPORT SHURTCUTS
    def write(self, data):
        self.transport.write(data)
            
    def writelines(self, lines):
        '''Write an iterable of bytes. It is a proxy to
:meth:`Transport.writelines`'''
        self.transport.writelines(lines)
        

class ChunkResponse(ProtocolResponse):
    
    def __init__(self, protocol):
        super(ChunkResponse, self).__init__(protocol)
        self._buffer = bytearray()  
    
    def feed(self, data):
        self._buffer.extend(data)
        message = self.decode(bytes(self._buffer))
        if message is not None:
            self.responde(message)
        
    def decode(self, data):
        raise NotImplementedError
    
    def responde(self, message):
        '''Write back to the client or server'''
        self.write(message)
        self._finished = True
    
        
class Protocol(object):
    '''Pulsar :class:`Protocol` conforming with pep-3156_.
It can be used for both client and server sockets.

* a *client* protocol is for clients connecting to a remote server.
* a *server* protocol is for socket created from an **accept**
  on a :class:`Server`.

.. attribute:: address

    Address of the client, if this is a server, or of the remote
    server if this is a client.
    
.. attribute:: transport

    The :class:`Transport` for this :class:`Protocol`. This is obtained once
    the :meth:`connection_made` is invoked.
    
.. attribute:: response_factory

    A factory of :class:`ProtocolResponse` instances for this :class:`Protocol`
    
.. attribute:: processed

    Number of separate requests processed by this protocol.
    
.. attribute:: current_response

    The :class:`ProtocolResponse` currently handling incoming data.
'''
    _transport = None
    response_factory = None
    
    def __init__(self, address, response_factory=None):
        self._processed = 0
        self._address = address
        self._current_response = None
        self.on_connection_lost = Deferred()
        if response_factory:
            self._response_factory = response_factory
    
    def __repr__(self):
        return str(self._address)
    
    def __str__(self):
        return self.__repr__()
        
    @property
    def processed(self):
        return self._processed
    
    @property
    def address(self):
        return self._address
    
    @property
    def transport(self):
        return self._transport
        
    @property
    def event_loop(self):
        if self.transport:
            return self.transport.event_loop
    
    @property
    def sock(self):
        if self.transport:
            return self.transport.sock
        
    @property
    def closed(self):
        return self._transport.closed if self._transport else True
    
    @property
    def response_factory(self):
        return self._response_factory
    
    @property
    def current_response(self):
        self._current_response
    
    ############################################################################
    ###    PEP 3156 METHODS
    def connection_made(self, transport):
        """Called when a connection is made. The argument is the
:class:`Transport` representing the connection.
To send data, call its :meth:`Transport.write` or
:meth:`Transport.writelines` method.
To receive data, wait for :meth:`data_received` calls.
When the connection is closed, :meth:`connection_lost` is called."""
        self._transport = transport
        if self._current_response is not None:
            self._current_response.on_connect()
            self._current_response.begin()

    def data_received(self, data):
        """Called by the :attr:`transport` when data is received.
By default it feeds the *data*, a bytes object, into the
:attr:`current_response` attribute."""
        while data:
            response = self._current_response
            if response is not None and response.finished():
                response = None
            if response is None:
                self._processed += 1
                self._current_response = response = self._response_factory(self)
            data = response.feed(data)
            if data and not response.finished():
                # if data is returned from the response feed method and the
                # response has not done yet raise a Protocol Error
                raise ProtocolError
            
    def upgrade(self, response_factory):
        '''Update the :attr:`response_factory` attribute with a new
:class:`ProtocolResponse`. This function can be used when the protocol
specification changes during a response (an example is a WebSocket
response).'''
        self._response_factory = response_factory
            
    def eof_received(self):
        """Called when the other end calls write_eof() or equivalent."""

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        self.on_connection_lost.callback(exc)
        
    ############################################################################
    ###    PULSAR METHODS
    def set_response(self, response):
        '''Set a new response instance on this protocol. If a response is
already available it raises an exception.'''
        assert self._current_response is None, "protocol already in response"
        self._current_response = response
        if self._transport is not None:
            self._current_response.begin()
            
    ############################################################################
    ###    TRANSPORT METHODS SHORTCUT
    def close(self):
        if self._transport:
            self._transport.close()
    
    def abort(self):
        if self._transport:
            self._transport.abort()
    
