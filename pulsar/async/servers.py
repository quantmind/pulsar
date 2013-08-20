'''Servers creating Transports'''
import socket
from functools import partial

from pulsar import create_socket, ProtocolError
from pulsar.utils.sockets import create_socket, get_transport_type, wrap_socket
from pulsar.utils.internet import format_address

from .defer import EventHandler
from .protocols import Connection, Producer, TransportProxy

__all__ = ['Server', 'TcpServer']
    

class Server(Producer):
    '''A base class for Servers listening on a socket.
    
An instance of this class is a :class:`Producer` of server sockets.
    
.. attribute:: consumer_factory

    Factory of :class:`ProtocolConsumer` handling the server sockets.
    '''    
    ONE_TIME_EVENTS = ('start', 'stop')
    MANY_TIMES_EVENTS = ('connection_made', 'pre_request','post_request',
                         'connection_lost')
    consumer_factory = None
    
    def __init__(self, event_loop, host=None, port=None, sock=None,
                 consumer_factory=None, name=None, **kw):
        super(Server, self).__init__(**kw)
        self._name = name or self.__class__.__name__
        self._event_loop = event_loop
        self._host = host
        self._port = port
        self._sock = sock
        if consumer_factory:
            self.consumer_factory = consumer_factory
    
    def close(self):
        '''Stop serving and close the listening socket.'''
        raise NotImplementedError
    
    def protocol_factory(self):
        return self.new_connection(self.consumer_factory)
        
    @property
    def event_loop(self):
        '''The :class:`EventLoop` running the server'''
        return self._event_loop
    
    @property
    def sock(self):
        '''The socket receiving connections.'''
        return self._sock
    
    @property
    def address(self):
        '''Server address, where clients send requests to.'''
        return self._sock.getsockname()
        

     
class TcpServer(Server):
    '''A :class:`Producer` for TCP servers.
    
.. attribute:: consumer_factory

    Callable or a :class:`ProtocolConsumer` class for producing
    :class:`ProtocolConsumer` which handle the receiving, decoding and
    sending of data.
    
'''
    def start_serving(self, backlog=100):
        '''Start serving the Tcp socket.
        
It returns a :class:`Deferred` calldback when the server is serving
the socket'''
        res = self._event_loop.start_serving(self.protocol_factory,
                                             host=self._host,
                                             port=self._port,
                                             sock=self._sock,
                                             backlog=backlog)
        return res.add_callback(self._got_sockets, lambda f: f.log())\
                  .add_callback(lambda r: self.fire_event('start'))
    
    def stop_serving(self):
        '''Stop serving the :class:`Server.sock`'''
        if self._sock:
            self._event_loop.stop_serving(self._sock)
            self.fire_event('stop')
    
    def close(self):
        self.stop_serving()
        self._sock = None
            
    def _got_sockets(self, sockets):
        self._sock = sockets[0]
        self.event_loop.logger.info('%s serving on %s', self._name,
                                    format_address(self._sock.getsockname()))
        return self._sock