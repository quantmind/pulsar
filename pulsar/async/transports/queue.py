from functools import partial
from multiprocessing.queues import Empty, Queue

from pulsar.utils.system import IObase
from pulsar.async.defer import async

from . import transport
from . import servers
from . import protocols


__all__ = ['QueueServer',
           'MessageQueue',
           'PythonMessageQueue',
           'Empty',
           'Queue',
           'IOQueue']
    
    
class MessageQueue(transport.Transport):
    ''':class:`Transport` class for message queues.'''
    event_loop = None
    def put(self, message):
        raise NotImplementedError
    
    def get(self, timeout=1):
        raise NotImplementedError
    
    def poller(self, server):
        pass
    
    def write(self, data):
        self.put(data)
        
    def size(self):
        '''Approximate size of the message queue.'''
        raise NotImplementedError
        
    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()
    

class Task(protocols.ProtocolConsumer):
    
    def __init__(self, request_factory, connection):
        self.request_factory = request_factory
        super(Task, self).__init__(connection)
        
    @async()
    def data_received(self, data):
        request = yield self.request_factory(data)
        result = yield request.start()
        yield self.finished(result)
    
    def finished(self, result):
        self.connection.connection_lost(None)
    
    
class QueueServer(servers.Server):
    '''A server'''
    def __init__(self, backlog=1, **kwargs):
        self.backlog = backlog
        super(QueueServer, self).__init__(**kwargs)
    
    def can_poll(self):
        '''The poller can pool only when the transport is available and
the total number of concurrent connections is les then the maximum'''
        return self.transport and\
                     self.concurrent_connections < self.backlog
        
    def connection_made(self, transport):
        self.consumer_factory = partial(Task, self.consumer_factory)
        self._transport = transport
        transport.event_loop.add_reader(transport.fileno(), self.data_received)
            
    def data_received(self, request):
        protocol = self.new_connection(self.transport.fileno(),
                                       consumer_factory=self.consumer_factory)
        protocol.connection_made(self.transport)
        protocol.data_received(request)
        
        
################################################################################
##    MESSAGE QUEUE BASED ON PYTHON MUTIPROCESSING QUEUE
################################################################################
class PythonMessageQueue(MessageQueue):
    '''A :class:`MessageQueue` based on python multiprocessing Queue.
This queue is not socket based therefore it requires a specialised IO poller,
the file descriptor is a dummy number and the waker is `self`.
The waker, when invoked via the :meth:`wake`, reduces the poll timeout to 0
so that the :meth:`get` method returns as soon possible.'''
    def __init__(self):
        self._wakeup = True
        self._queue = Queue()
        
    def poller(self, server):
        return IOQueue(self, server)
    
    def get(self, timeout=0.5):
        '''Get an item from the queue.'''
        if self._wakeup:
            self._wakeup = False
            return self._queue.get(block=False)
        else:
            return self._queue.get(timeout=timeout)

    def put(self, message):
        self._queue.put(message)
        
    def size(self):
        try:
            return self._queue.qsize()
        except NotImplementedError: #pragma    nocover
            return 0
        
    def fileno(self):
        '''dummy file number'''
        return 1
    
    def close(self, async=False, exc=None):
        pass
    
    def wake(self):
        '''Waker implementation. This message queue is its own waker.'''
        self._wakeup = True
        

class TaskFactory:
    
    def __init__(self, fd, eventloop, read=None, **kwargs):
        self.fd = fd
        self.eventloop = eventloop
        self.handle_read = None
        self.add_reader(read)
    
    def add_connector(self, callback):
        pass
        
    def add_reader(self, callback):
        if not self.handle_read:
            self.handle_read = callback
        else:
            raise RuntimeError("Already reading!")
        
    def add_writer(self, callback):
        pass
        
    def remove_connector(self):
        pass
    
    def remove_writer(self):
        pass
    
    def remove_reader(self):
        '''Remove reader and return True if writing'''
        self.handle_read = None
    
    def __call__(self, request):
        return self.handle_read(request)


class IOQueue(IObase):
    '''Epoll like class for a IO based on queues rather than sockets.
The interface is the same as the python epoll_ implementation.

.. _epoll: http://docs.python.org/library/select.html#epoll-objects'''
    cpubound = True
    fd_factory = TaskFactory
    def __init__(self, queue, app):
        self._queue = queue
        self._app = app
        self._handler = None

    def register(self, fd, events=None):
        pass

    def modify(self, fd, events=None):
        pass

    def unregister(self, fd):
        pass

    def poll(self, timeout=0.5):
        '''Wait for events. timeout in seconds (float)'''
        if not self._app.can_poll():
            return ()
        try:
            request = self._queue.get(timeout=timeout)
        except (Empty, IOError, TypeError, EOFError):
            return ()
        return ((self._queue.fileno(), request),)
        
    def install_waker(self, event_loop):
        # install a dummy callback so that the event_loop keep on running
        self._handler = event_loop.call_every(self._every_loop)
        return self._queue
    
    def _every_loop(self):
        pass