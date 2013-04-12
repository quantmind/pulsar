'''Servers creating Transports'''
import socket
from functools import partial

from pulsar import create_socket, ProtocolError
from pulsar.utils.sockets import create_socket, get_transport_type, wrap_socket
from pulsar.async.defer import EventHandler

from .protocols import Connection, Producer
from .transport import TransportProxy, create_transport, LOGGER

__all__ = ['create_server', 'Server']

     
class Server(Producer, TransportProxy):
    '''A :class:`Producer` for all server's listening for connections.
It is a producer of :class:`Transport` for server protocols.
    
.. attribute:: transport:

    The :class:`Transport` listening for client connections.
    
.. attribute:: consumer_factory

    Callable or a :class:`ProtocolConsumer` class for producing
    :class:`ProtocolConsumer` which handle the receiving, decoding and
    sending of data.
    
.. attribute:: event_loop

    The :class:`EventLoop` running the server.
    
.. attribute:: address

    Server address, where clients send requests to.
    
.. attribute:: timeout

    A timeout in seconds for idle connections
'''
    ONE_TIME_EVENTS = ('start', 'finish')
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request','post_request',
                         'connection_lost')
    consumer_factory = None
    
    def __init__(self, consumer_factory=None, **kw):
        super(Server, self).__init__(**kw)
        if consumer_factory:
            self.consumer_factory = consumer_factory
        
    def data_received(self, sock, address):
        '''Create a new server :class:`Protocol` ready to serve its client.'''
        # Build the connection
        sock = wrap_socket(self.transport.TYPE, sock)
        protocol = self.new_connection(address, self.consumer_factory)
        transport = create_transport(protocol, sock=sock,
                                     event_loop=self.event_loop)
        transport.add_reader()
        protocol.connection_made(transport)
        return protocol
        
    def datagram_received(self, data, address):
        raise NotImplementedError
    
    @property
    def timeout(self):
        return self._timeout
    
    @property
    def address(self):
        return self.transport.address
    
    def connection_made(self, transport):
        self._transport = transport
        transport.add_listener()
        LOGGER.debug('Registered server listening on %s', self)
        self.fire_event('start')
        
    def connection_lost(self, exc):
        self.close_connections()
        self.fire_event('finish')
        
    @classmethod
    def create(cls, event_loop, sock=None, address=None, backlog=1024,
               name=None, close_event_loop=None, **kw):
        '''Create a new server!'''
        sock = create_socket(sock=sock, address=address, bindto=True,
                             backlog=backlog)
        transport_type = get_transport_type(sock.TYPE).transport
        server = cls(**kw)
        transport = transport_type(event_loop, sock, server)
        server.connection_made(transport)
        if close_event_loop:
            server.bind_event('finish',
                                lambda exc: server.event_loop.stop())
        return server

create_server = Server.create
