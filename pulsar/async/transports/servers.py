'''Servers creating Transports'''
import socket
from functools import partial

from pulsar import create_socket, ProtocolError
from pulsar.utils.sockets import create_socket, get_transport_type, wrap_socket
from pulsar.utils.pep import get_event_loop, set_event_loop, new_event_loop
from pulsar.async.defer import EventHandler
from pulsar.async.access import PulsarThread, NOTHING

from .protocols import Connection, Producer
from .transport import TransportProxy, create_transport, LOGGER

__all__ = ['create_server', 'Server']

     
class Server(Producer, TransportProxy):
    '''A :class:`Producer` for all server's listening for connections
on a socket. It is a producer of :class:`Transport` for server protocols.
    
.. attribute:: transport:

    The :class:`Transport` listening for client connections.
    
.. attribute:: consumer_factory

    Callable or a :class:`ProtocolConsumer` class used
    to override the :class:`Protocol.consumer_factory` attribute. It produces
    :class:`ProtocolConsumer` which handle the receiving, decoding and
    sending of data.
    
.. attribute:: connection_factory

    A factory producing the :class:`Connection` for a socket created
    from a connection of a remote client with this server. This is a function
    or a :class:`Connection` class which accept two arguments,
    the client address and the :attr:`consumer_factory` attribute.
    This attribute is used in the :meth:`create_connection` method.
    **By default pulsar uses a connection appropiate to the socket type** .
    There shouldn't be any reason to change the default,
    it is here just in case.
    
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
        protocol.copy_many_times_events(self)
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
        LOGGER.debug('Registered server listening on %s', self)
        self.fire_event('start')
        
    def connection_lost(self, exc):
        self.close_connections()
        self.fire_event('finish')
        
    @classmethod
    def create(cls, eventloop=None, sock=None, address=None, backlog=1024,
               name=None, close_event_loop=None, **kw):
        '''Create a new server!'''
        sock = create_socket(sock=sock, address=address, bindto=True,
                             backlog=backlog)
        transport_type = get_transport_type(sock.TYPE).transport
        eventloop = loop = eventloop or get_event_loop()
        server = cls(**kw)
        transport = None
        # The eventloop is cpubound
        if getattr(eventloop, 'cpubound', False):
            loop = get_event_loop()
            if loop is None:
                # No event loop available in the current thread.
                # Create one and set it as the event loop
                loop = new_event_loop()
                set_event_loop(loop)
                transport = transport_type(loop, sock, server, as_server=True)
                # Shutdown eventloop when server closes
                close_event_loop = True
                # start the server on a different thread
                eventloop.call_soon_threadsafe(_start_on_thread, name, server)
        if transport is None:
            transport = transport_type(loop, sock, server, as_server=True)
        server.connection_made(transport)
        if close_event_loop:
            server.bind_event('finish',
                                lambda exc: server.event_loop.stop())
        return server

create_server = Server.create

################################################################################
##    INTERNALS
def _start_on_thread(name, server):
    # we are on the actor request loop thread, therefore the event loop
    # should be already available if the tne actor is not CPU bound.
    PulsarThread(name=name, target=server.event_loop.run).start()
    
