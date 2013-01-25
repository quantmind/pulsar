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
    MANY_TIMES_EVENTS = ('pre_request', 'post_request')
    consumer_factory = None
    
    def __init__(self, transport, consumer_factory=None, timeout=None, **kw):
        super(Server, self).__init__(**kw)
        TransportProxy.__init__(self, transport)
        self._timeout = timeout
        if consumer_factory:
            self.consumer_factory = consumer_factory
        if self.transport.TYPE == socket.SOCK_DGRAM:
            self.transport.add_listener(self.datagram_received)
        else:
            self.transport.add_listener(self.connection_received)
        # when transport starts closing, close connections
        transport.bind_event('closing', lambda s: self.close_connections())
        LOGGER.debug('Registered server listening on %s', self)
        
    def connection_received(self, sock_addr):
        '''Create a new server :class:`Protocol` ready to serve its client.'''
        # Build the protocol
        sock, addr = sock_addr
        sock = wrap_socket(self.transport.TYPE, sock)
        transport = create_transport(sock=sock, event_loop=self.event_loop,
                                     timeout=self.timeout)
        #Create the connection
        connection = self.new_connection(addr, self.consumer_factory)
        connection.connection_made(transport)
        return connection
        
    def datagram_received(self, data_addr):
        raise NotImplementedError
    
    @property
    def timeout(self):
        return self._timeout
        
    @classmethod
    def create(cls, eventloop=None, sock=None, address=None, backlog=1024,
               name=None, close_event_loop=None, **kw):
        '''Create a new server!'''
        sock = create_socket(sock=sock, address=address, bindto=True,
                             backlog=backlog)
        transport_type = get_transport_type(sock.TYPE).transport
        eventloop = loop = eventloop or get_event_loop()
        transport = None
        # The eventloop is cpubound
        if getattr(eventloop, 'cpubound', False):
            loop = get_event_loop()
            if loop is None:
                # No event loop available in the current thread.
                # Create one and set it as the event loop
                loop = new_event_loop()
                set_event_loop(loop)
                transport = transport_type(loop, sock)
                # Shutdown eventloop when server closes
                close_event_loop = True
                # start the server on a different thread
                eventloop.call_soon_threadsafe(_start_on_thread, name, server)
        transport = transport or transport_type(loop, sock)
        server = cls(transport, **kw)
        if close_event_loop:
            server.transport.bind_event('connection_lost',
                                        lambda exc: server.event_loop.stop())
        return server

create_server = Server.create

################################################################################
##    INTERNALS
def _start_on_thread(name, server):
    # we are on the actor request loop thread, therefore the event loop
    # should be already available if the tne actor is not CPU bound.
    PulsarThread(name=name, target=server.event_loop.run).start()
    
