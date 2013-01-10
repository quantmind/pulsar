from .transports import ServerTransport
from .mixins import ConcurrentServer

__all__ = ['Protocol', 'ProtocolResponse']


class ProtocolResponse(object):
    '''A :class:`Protocol` response is responsible for parsing incoming data.'''
    def __init__(self, protocol):
        self.protocol = protocol
            
    @property
    def event_loop(self):
        return self.protocol.event_loop
    
    @property
    def sock(self):
        return self.protocol.sock
        
    def feed(self, data):
        raise NotImplementedError
    
    def finished(self):
        raise NotImplementedError
    
    def write(self, data):
        if is_async(data):
            self.event_loop.call_soon(self.write, data)
        else:
            self.protocol.transport.write(data)


class Protocol(object):
    '''Base class for a pulsar :class:`Protocol`.
    
.. attribute:: transport

    The :class:`Transport` for this :class:`Protocol`
    
.. attribute:: response

    The :class:`ProtocolResponse` factory for this :class:`Protocol`
'''
    transport = None
    response = None
        
    @property
    def event_loop(self):
        if self.transport:
            return self.transport.event_loop
    
    @property
    def sock(self):
        if self.transport:
            return self.transport.sock
    
    def connection_made(self, transport):
        """Called when a connection is made.

        The argument is the transport representing the connection.
        To send data, call its write() or writelines() method.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """
        self.transport = transport

    def data_received(self, data):
        """Called by the :attr:`transport` when some data is received.
The argument is a bytes object."""
            
    def eof_received(self):
        """Called when the other end calls write_eof() or equivalent."""

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
    
    
class ClientProtocol(Protocol):
    
    def __init__(self, address, response=None):
        self._processed = 0
        self.address = address
        self.current_response = None
        if response:
            self.response = response
            
    @property
    def processed(self):
        return self._processed
    
    def data_received(self, data):
        """Called by the :attr:`transport` when some data is received.
The argument is a bytes object."""
        response = self.current_response
        if response is not None and response.finished():
            response = None
        if response is None:
            self._processed += 1
            self.current_response = response = self.response(self)
        data = response.feed(data)
        if data:
            self.data_received(data)
            
            
class ServerProtocol(Protocol, ConcurrentServer):
    '''Base class for all Server's protocols.
    
.. attribute:: protocol

    The :class:`Protocol` for a socket created from a connection of a remote
    client with this server. It is usually a subclass of
    :class:`ClientProtocol`.
'''
    protocol = ClientProtocol
    
    def create_transport(self, event_loop, sock):
        ServerTransport(event_loop, sock, self)