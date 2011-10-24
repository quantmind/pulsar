import io
import logging
import socket
import time
from multiprocessing.queues import Empty, Queue

from pulsar import create_connection, MailboxError, socket_pair, wrap_socket
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.py2py3 import pickle


__all__ = ['mailbox','Mailbox','SocketServerMailbox','IOQueue','Empty','Queue']

crlf = b'\r\n'


def mailbox(address = None, id = None, queue = None):
    '''Creates a :class:`Mailbox` instances for :class:`Actor` instances.
If an address is provided, the communication is implemented using a socket,
otherwise a queue is used.'''   
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


def serverSocket():
    '''Create an asynchronous server inbox. This is TCP socket ready for
accepting messages from other actors.'''
    # get a socket pair
    w,s = socket_pair(backlog = 64)
    s.setblocking(True)
    r, _ = s.accept()
    r.close()
    w.close()
    s.setblocking(False)
    return s


class Mailbox(object):
    '''A mailbox for :class:`Actor` instances. They are the tool which
allows actors to communicate with each other in a share-nothing architecture.
The implementation of a mailbox can be of two types:
 
* Socket
* Distributes queue

Mailboxes are setup before a new actor is forked during
the initialization of :class:`ActorImpl`.
'''
    actor = None
    type = None
    
    def register(self, actor, inbox = True):
        '''register *actor* with the mailbox.
In doing so the :attr:`Actor.ioloop`
add the mailbox as read handler which wakes up on events to invoke
:meth:`on_message`.
This method is invoked during :meth:`Actor.start` after initialisation
of the :attr:`Actor.ioloop`'''
        self.actor = actor
        self.type = 'inbox' if inbox else 'outbox'
        self.on_actor()
        if inbox:
            actor.ioloop.add_handler(self,
                                     self.on_message,
                                     actor.ioloop.READ)
            # the actor need to acknowledge the arbiter
            if actor.impl != 'monitor':
                address = self.address()
                actor.log.debug('Sending address {0} to arbiter'\
                                 .format(address))
                actor.ioloop.add_callback(
                    lambda : actor.arbiter.send(self.actor,
                                    'inbox_address', address))
    
    def name(self):
        return 'mailbox'
    
    def __str__(self):
        if self.actor:
            return '{0} {1} {2}'.format(self.actor,self.name(),self.type)
        else:
            return 'mailbox'
    
    def clone(self):
        '''Get an instance of the mailbox to be used on a different process
domain. By default it return ``self``.'''
        return self
    
    def __repr__(self):
        return self.__str__()
    
    def on_actor(self):
        pass
    
    def on_message(self, fd, events):
        '''Handle the message by parsing it and invoking
:meth:`Actor.message_arrived`'''
        for message in self.read_message(fd, events):
            self.actor.message_arrived(message)
        
    def address(self):
        '''The address of the mailbox'''
        return None
    
    def fileno(self):
        '''Return the file descriptor of the mailbox.'''
        raise NotImplementedError
    
    def put(self, request):
        '''Put a :class:`ActorMessage` into the mailbox. This function is
 available when the mailbox is acting as an outbox.'''
        raise NotImplementedError
    
    def read_message(self, fd, events):
        raise NotImplementedError
    
    def close(self):
        pass
        

class QueueMailbox(Mailbox):
    '''A mailbox handled by a queue.'''
    def __init__(self, id, queue = None):
        self.id = id
        self.queue = queue
    
    def name(self):
        return 'queue "{0}"'.format(self.id)
    
    def fileno(self):
        return self.id
    
    def put(self, request):
        try:
            r = (request.receiver,pickle.dumps(request))
        except:
            self.actor.log.critical('Could not serialize {0}'.format(request),
                                    exc_info = True)
            return
        try:
            self.queue.put((self.id,r))
        except (IOError,TypeError):
            pass
        
    def read_message(self, fd, events):
        aid = events[0]
        # if the id is the same as the actor, than this is a message to yield
        if aid == self.actor.aid: 
            yield pickle.loads(events[1])
        else:
            # Otherwise put it back in the queue
            try:
                self.queue.put((self.id,events))
            except (IOError,TypeError):
                pass
        
    
class SocketMailbox(Mailbox):
    '''A socket outbox for :class:`Actor` instances. This outbox
send messages to a :class:`SocketServerMailbox`.'''
    def __init__(self, address):
        self._address = address
        self.sock = None
        
    def name(self):
        return str(self.sock or self._address)
    
    def address(self):
        return self._address
    
    def fileno(self):
        return self.sock.fileno()
            
    def __getstate__(self):
        d = self.__dict__.copy()
        d['sock'] = None
        return d
        
    def on_actor(self):
        if self.type == 'inbox':
            raise ValueError('Trying to use {0} as inbox'\
                             .format(self.__class__.__name__))
        try:
            self.sock = create_connection(self._address,blocking=True)
        except Exception as e:
            raise MailboxError('Cannot register {0}. {1}'.format(self,e))
        
    def put(self, request):
        request = pickle.dumps(request) + crlf
        try:
            return self.sock.send(request)
        except socket.error as e:
             self.close()
    
    def read_message(self, fd, events, client = None):
        raise MailboxError('Cannot read messages. This is an outbox only.')        
        
    def close(self):
        if self.sock:
            self.sock.close()


class SocketServerClient(object):
    __slots__ = ('sock',)
    def __init__(self, sock):
        self.sock = sock
        
    def fileno(self):
        return self.sock.fileno()
    
    def __str__(self):
        return '{0} inbox client'.format(self.sock)
    
    
class SocketServerMailbox(Mailbox):
    '''An inbox for :class:`Actor` instances. If an address is provided,
the communication is implemented using a socket, otherwise a unidirectional
pipe is created.'''
    def __init__(self):
        self.sock = None
        self.clients = {}
        
    def name(self):
        return str(self.sock)
            
    def address(self):
        return self.sock.getsockname()
    
    def fileno(self):
        return self.sock.fileno()
        
    def put(self, request):
        raise MailboxError('Cannot put messages')
    
    def clone(self):
        return None
    
    def on_actor(self):
        self.sock = serverSocket()
        self.buffer = bytearray()
        if self.type == 'outbox':
            raise ValueError('Trying to use {0} as outbox'\
                             .format(self.__class__.__name__))
    
    def read_message(self, fd, events):
        '''Called when a new message has arrived.'''
        ioloop = self.actor.ioloop
        client = self.clients.get(fd)
        if not client:
            client,_ = self.sock.accept()
            if not client:
                self.actor.log.debug('Still no client. Aborting')
                return
            client = wrap_socket(client)
            client.setblocking(True)
            self.clients[client.fileno()] = client
            #self.actor.log.debug('Got inbox event on {0}, {1}'.format(fd,client))
            ioloop.add_handler(SocketServerClient(client),
                               self.on_message,
                               ioloop.READ)
            return
        
        length = io.DEFAULT_BUFFER_SIZE
        try:
            chunk = client.recv(length)
        except socket.error:
            chunk = None
        
        toclose = False
        if chunk:
            self.buffer.extend(chunk)
            while len(chunk) > length:
                chunk = client.recv(length)
                if not chunk:
                    break
                else:
                    self.buffer.extend(chunk)
        else:
            toclose = True
            
        while self.buffer:
            p = self.buffer.find(crlf)
            if p >= 0:
                msg = self.buffer[:p]
                del self.buffer[0:p+2]
                yield pickle.loads(bytes(msg))
            else:
                break
            
        if toclose:
            ioloop.remove_handler(client)
            client.close()
        
    def close(self):
        if self.sock:
            self.actor.log.debug('shutting down {0} inbox'.format(self.actor))
            for c in self.clients:
                try:
                    c.close()
                except:
                    pass
            self.sock.close()


class QueueWaker(object):
    
    def __init__(self, queue):
        self._queue = queue
        self._fd = 'waker'
        
    def __str__(self):
        return '{0} {1}'.format(self.__class__.__name__,self._fd)
    
    def fileno(self):
        return self._fd
    
    def wake(self):
        try:
            self._queue.put((self._fd,None))
        except (IOError,TypeError):
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
        except (Empty,IOError,TypeError):
            return self._empty
        
        return (event,)
    
    def waker(self):
        return QueueWaker(self._queue)
    
    
        