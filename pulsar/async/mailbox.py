import io
import logging
from multiprocessing.queues import Empty, Queue

from pulsar import create_connection, MailboxError
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.mixins import NonePickler
from pulsar.utils.py2py3 import pickle


__all__ = ['mailbox','Mailbox','IOQueue','Empty','Queue']


def mailbox(address = None, id = None, queue = None, stream = None):
    '''Creates a :class:`Mailbox` instances for :class:`Actor` instances.
If an address is provided, the communication is implemented using a socket,
otherwise a queue is used. If stream is provided, this is the arbiter socket.'''   
    if address:
        if id:
            raise ValueError('Mailbox with address and id is not supported')
        return SocketMailbox(address)
    elif id and queue:
        return QueueMailbox(id,queue)
    elif stream:
        return SocketServerMailbox(stream)
    else:
        raise ValueError('Cannot obtain a valid mailbox')
        o = PipeOutbox()


class Mailbox(object):
    '''A mailbox for :class:`Actor` instances. If an address is provided,
the communication is implemented using a socket, otherwise a unidirectional
pipe is created. Mailboxes are setup before a new actor is forked during
the initialization of :class:`ActorImpl`.'''        
    def setup(self):
        '''Called after forking to setup the mailbox.'''
        pass
    
    def set_actor(self, actor):
        '''Set the actor in the mailbox. Indoing so the actor :class:`IOLoop`
add the mailbox as handler which wakes up on events to invoke
:meth:`on_message`.'''
        self.actor = actor
        actor.ioloop.add_handler(self,
                                 self.on_message,
                                 actor.ioloop.READ)
    
    def on_message(self, fd, events, **kwargs):
        message = self.read_message(fd, events, **kwargs)
        if message:
            self.actor.message_arrived(message)
        
    def address(self):
        '''The address of the mailbox'''
        return None
    
    def __setstate__(self, state):
        self.__dict__ = state
        self.setup()
        
    def put(self, request):
        raise NotImplementedError
    
    def read_message(self,  fd, events):
        raise NotIMplementedError
    
    def close(self):
        pass
        

class QueueMailbox(Mailbox):
    '''An mailbox handled by a queue.'''
    def __init__(self, id, queue = None):
        self.id = id
        self.queue = queue
        
    def fileno(self):
        return self.id
    
    def put(self, request):
        try:
            self.queue.put((self.id,request))
        except:
            pass
        
    def read_message(self, fd, events):
        return events
        
        
class PipeMailbox(Mailbox):
    '''An outbox for :class:`Actor` instances. If an address is provided,
the communication is implemented using a socket, otherwise a unidirectional
pipe is created.'''
    def __init__(self):
        self.reader, self.writer = Pipe(duplex = False)
        
    def fileno():
        return self.sock.fileno()
            
    
class SocketMailbox(Mailbox):
    '''An inbox for :class:`Actor` instances.'''
    def __init__(self, address):
        self._address = address
        self.setup()
        #self.sock = None
        
    def address(self):
        return self._address
    
    def fileno(self):
        return self.sock.fileno()
            
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('sock',None)
        return d
        
    def setup(self):
        self.sock = create_connection(self._address,blocking=True)
        
    def put(self, request):
        request = pickle.dumps(request)
        return self.sock.send(request)
    
    def read_message(self, fd, events, client = None):
        raise MailboxError('Cannot read messages')        
        
    def close(self):
        self.actor.log.debug('shutting down {0} outbox'.format(self.actor))
        self.sock.close()


def getNone(*args,**kwargs):
    return None

class SocketServerMailbox(NonePickler,Mailbox):
    '''An outbox for :class:`Actor` instances. If an address is provided,
the communication is implemented using a socket, otherwise a unidirectional
pipe is created.'''
    def __init__(self, stream):
        self.stream = stream
        self.clients = set()
        
    def __str__(self):
        return '{0} socket server mailbox'.format(self.stream.actor)
    __repr__ = __str__
    
    def set_actor(self, actor):
        self.stream.set_actor(actor)
        super(SocketServerMailbox,self).set_actor(actor)
        
    def address(self):
        return self.stream.getsockname()
    
    def fileno(self):
        return self.stream.fileno()
        
    def put(self, request):
        raise MailboxError('Cannot put messages')
    
    def read_message(self, fd, events, client = None):
        '''Called when a new message has arrived.'''
        length = io.DEFAULT_BUFFER_SIZE
        if not client:
            client,addr = self.stream.socket.accept()
            if not client:
                return
            self.register_client(client)
            
        chunk = client.recv(length)
        msg = bytearray(chunk)
        while len(chunk) > length:
            chunk = client.read(length)
            msg.extend(chunk)
        if msg:
            return pickle.loads(bytes(msg))

    def register_client(self, client):
        self.clients.add(client)
        client.setblocking(True)
        actor = self.stream.actor
        self.stream.ioloop.add_handler(client,
            lambda fd, events : self.on_message(fd, events, client = client),
            self.stream.ioloop.READ)
        
    def close(self):
        self.actor.log.debug('shutting down {0} inbox'.format(self.actor))
        for c in self.clients:
            try:
                c.close()
            except:
                pass
        self.stream.close()


class QueueWaker(object):
    
    def __init__(self, queue):
        self._queue = queue
        self._fd = 'waker'
        
    def fileno(self):
        return self._fd
    
    def wake(self):
        try:
            self._queue.put((self._fd,None))
        except IOError:
            pass
        
    def consume(self):
        pass
    
    def close(self):
        pass
    
        
class IOQueue(object):
    '''Epoll like class for a IO based on queues.
The interface is the same as the python epoll_ implementation.

.. _epoll: http://docs.python.org/library/select.html#epoll-objects'''
    def __init__(self, queue):
        self._queue = queue
        self._fds = set()
        self._empty = ()
        
    @property
    def queue(self):
        '''The underlying distributed queue used for I/O.'''
        return self._queue

    def register(self, fd, events = None):
        '''Register a fd descriptor with the io queue object'''
        self._fds.add(fd)
                
    def modify(self, fd, events = None):
        '''Modify a registered file descriptor'''
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        '''Remove a registered file descriptor from the ioqueue object.. '''
        self._fds.discard(fd)
    
    def poll(self, timeout = 0.5):
        '''Wait for events. timeout in seconds (float)'''
        try:
            event = self._queue.get(timeout = timeout)
        except Empty:
            return self._empty
        except IOError:
            return self._empty
        
        return (event,)
    
    def waker(self):
        return QueueWaker(self._queue)
    
    
        