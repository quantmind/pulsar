from functools import partial

from pulsar.utils.queue import IOQueue
from pulsar.async.defer import make_async

from . import transport
from . import servers
from . import protocols

__all__ = ['QueueServer', 'QueueTransport']



            
        
class QueueTransport(transport.Transport):
    event_loop = None
    def __init__(self, queue):
        self.queue = queue        
        
    def write(self, data):
        self.queue.put((self.fileno(), data))
            
    def fileno(self):
        return 1
    
    def close(self, async=False, exc=None):
        pass
    

class Task(protocols.ProtocolConsumer):
    
    def __init__(self, request_factory, connection):
        self.request_factory = request_factory
        super(Task, self).__init__(connection)
        
    def data_received(self, data):
        request = self.request_factory(data)
        make_async(request.start()).add_callback(self.finished,
                                  self.connection.connection_lost)
    
    def finished(self, result):
        self.connection.connection_lost(None)
    
    
class QueueServer(servers.Server):
    
    def __init__(self, backlog=1, **kwargs):
        self.backlog = backlog
        super(QueueServer, self).__init__(**kwargs)
        
    def poller(self, queue):
        return IOQueue(queue, self)
    
    def can_poll(self):
        '''The poller can pool only when the transport is available and
the total number of concurrent connections is les then the maximum'''
        return self.transport and\
                     self.concurrent_connections < self.backlog
        
    def connection_made(self, transport):
        transport.event_loop.add_reader(transport.fileno(), self.data_received)
        self.consumer_factory = partial(Task, self.consumer_factory)
        self._transport = transport
            
    def data_received(self, request):
        protocol = self.new_connection(self.transport.fileno(),
                                       consumer_factory=self.consumer_factory)
        protocol.connection_made(self.transport)
        protocol.data_received(request)
        