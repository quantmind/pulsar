import io
import sys
import logging
import socket
import time
import threading
from multiprocessing.queues import Empty, Queue

from pulsar import create_connection, MailboxError, socket_pair, wrap_socket
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.py2py3 import pickle

from .eventloop import IOLoop
from .defer import make_async, raise_failure, Failure


__all__ = ['mailbox','Mailbox','IOQueue','Empty','Queue']

crlf = b'\r\n'


def mailbox(actor, address = None):
    '''Creates a :class:`Mailbox` instances for :class:`Actor` instances.
If an address is provided, the communication is implemented using a socket,
otherwise a queue is used.'''   
    if address:
        return MailboxProxy(address)
    else:
        if actor.is_monitor():
            mailbox = MailBoxMonitor(actor)
        else:
            mailbox = Mailbox(actor)
        # Add the actor to the loop tasks
        mailbox.ioloop.add_loop_task(actor)
        return mailbox


def process_message(receiver, sender, message):
    '''Process *message* received by *receiver* by performing the message
action. If an acknowledgment is required, send back the result using
the receiver eventloop.'''
    ack = message.ack
    try:
        func = receiver.actor_functions.get(message.action,None)
        if func:
            ack = getattr(func,'ack',True)
            result = func(receiver, sender, *message.args, **message.kwargs)
        else:
            result = receiver.handle_message(sender, message)
    except Exception as e:
        result = Failure(sys.exc_info())
        if receiver.log:
            receiver.log.critical('Error while processing message: {0}.'\
                              .format(e), exc_info=True)
    finally:
        if ack:
            if sender:
                # Acknowledge the sender with the result.
                make_async(result).start(receiver.ioloop)\
                .add_callback(
                        lambda res : sender.send(receiver, 'callback',\
                                                 message.rid, res))\
                .add_callback(raise_failure)
            else:
                receiver.log.error('message "{0}" from an unknown actor "{1}"'\
                                   .format(message,message.sender))

def serverSocket():
    '''Create the inbox, a TCP socket ready for
accepting messages from other actors.'''
    # get a socket pair
    w,s = socket_pair(backlog = 64)
    s.setblocking(True)
    r, _ = s.accept()
    r.close()
    w.close()
    s.setblocking(False)
    return s
        
    
class MailboxProxy(object):
    '''A socket outbox for :class:`Actor` instances. This outbox
send messages to a :class:`SocketServerMailbox`.'''
    def __init__(self, address):
        try:
            self.sock = create_connection(address,blocking=True)
        except Exception as e:
            raise MailboxError('Cannot register {0}. {1}'.format(self,e))
    
    @property
    def address(self):
        return self.sock.getsockname()
    
    def fileno(self):
        return self.sock.fileno()
        
    def put(self, request):
        request = pickle.dumps(request) + crlf
        try:
            return self.sock.send(request)
        except socket.error as e:
             self.close()
    
    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None


class SocketServerClient(object):
    __slots__ = ('sock','buffer')
    def __init__(self, sock):
        sock.setblocking(True)
        self.sock = wrap_socket(sock)
        self.buffer = bytearray()
        
    def fileno(self):
        return self.sock.fileno()
    
    def __str__(self):
        return '{0} inbox client'.format(self.sock)
    
    def recv(self, actor):
        length = io.DEFAULT_BUFFER_SIZE
        sock = self.sock
        buffer = self.buffer
        try:
            chunk = sock.recv(length)
        except socket.error:
            chunk = None
        
        toclose = False
        if chunk:
            buffer.extend(chunk)
            while len(chunk) > length:
                chunk = sock.recv(length)
                if not chunk:
                    break
                else:
                    buffer.extend(chunk)
        else:
            toclose = True
            
        while buffer:
            p = buffer.find(crlf)
            if p >= 0:
                msg = buffer[:p]
                del buffer[0:p+2]
                try:
                    data = pickle.loads(bytes(msg))
                except pickle.UnpicklingError:
                    actor.log.error('Could not unpickle message',
                                    exc_info = True)
                    continue
                yield data
            else:
                break
            
        if toclose:
            actor.ioloop.remove_handler(self)
            sock.close()
    
    
class Mailbox(threading.Thread):
    '''An inbox for :class:`Actor` instances. If an address is provided,
the communication is implemented using a socket, otherwise a unidirectional
pipe is created.'''
    def __init__(self, actor):
        self.actor = actor
        self.sock = serverSocket()
        self.pending = {}
        self.clients = {}
        name = '{0} {1}:{2}'.format(actor,self.address[0],self.address[1])
        threading.Thread.__init__(self, name = name)
        self.daemon = True
        # If the actor has a ioqueue (CPU bound actor) we create a new ioloop
        if actor.ioqueue is not None and not actor.is_monitor():
            self.__hasloop = True
            self.__ioloop = IOLoop(pool_timeout = actor._pool_timeout,
                                   logger = actor.log,
                                   name = name)
        else:
            self.__hasloop = False
            self.__ioloop = actor.requestloop
        
        self.ioloop.add_handler(self,
                                self.on_message,
                                self.ioloop.READ)
        if actor.arbiter:
            self.register(actor.arbiter)
        # add start to the actor
        actor.requestloop.add_callback(self.start)
    
    @property
    def ioloop(self):
        return self.__ioloop
    
    @property
    def address(self):
        return self.sock.getsockname()
    
    def fileno(self):
        return self.sock.fileno()
    
    def register(self, actor):
        '''Register *actor* with this mailbox by sending its socket address.
        '''
        self.actor.link_actor(actor)
        return actor.send(self.actor,'mailbox_address',self.address)
        
    def on_message(self, fd, events):
        '''Handle the message by parsing it and invoking
:meth:`Actor.message_arrived`'''
        for message in self.read_message(fd, events):
            self.message_arrived(message)
    
    def read_message(self, fd, events):
        '''Called when a new message has arrived.'''
        ioloop = self.actor.ioloop
        client = self.clients.get(fd)
        if not client:
            client,_ = self.sock.accept()
            if not client:
                self.actor.log.debug('Still no client. Aborting')
                return ()
            client = SocketServerClient(client)
            self.clients[client.fileno()] = client
            #self.actor.log.debug('Got inbox event on {0}, {1}'.format(fd,client))
            ioloop.add_handler(client, self.on_message, ioloop.READ)
            return ()
            
        return client.recv(self.actor)
        
    def unregister(self, actor):
        if not self.ioloop.remove_loop_task(actor):
            actor.log.warn('"{0}" could not be removed from\
 eventloop'.format(self))
            
    def close(self):
        self.unregister(self.actor)
        if self.__hasloop:
            self.ioloop.stop()
        if self.sock:
            self.ioloop.log.debug('shutting down {0}'.format(self))
            for c in self.clients:
                try:
                    c.close()
                except:
                    pass
            self.ioloop.remove_handler(self)
            self.sock.close()

    def run(self):
        if self.__hasloop:
            self.ioloop.start()
            
    def message_arrived(self, message):
        '''A new :class:`ActorMessage` has arrived in the :attr:`Actor.inbox`.
Here we check the sender and the receiver (it may be not ``self`` if
``self`` is the  :class:`Arbiter`) and perform the message action.
If the message needs acknowledgment, send the result back.'''
        actor = self.actor
        sender = actor.get_actor(message.sender)
        receiver = actor.get_actor(message.receiver)
        #if not sender or not receiver:
        #    #if not sender:
        #    #    actor.log.warn('message "{0}" from an unknown actor "{1}"'\
        #    #                  .format(message,message.sender))
        if not receiver:
            actor.log.warn('message "{0}" for an unknown actor "{1}"'\
                              .format(message,message.receiver))
        else:
            return process_message(receiver,sender,message)
            

class MailBoxMonitor(object):
    
    def __init__(self, actor):
        self.actor = actor
        self.mailbox = actor.arbiter.mailbox
        
    @property
    def address(self):
        return self.mailbox.address
    
    @property
    def ioloop(self):
        return self.mailbox.ioloop
    
    def close(self):
        self.mailbox.unregister(self.actor)


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
        except (Empty,IOError,TypeError,EOFError):
            return self._empty
        
        return (event,)
    
    def waker(self):
        return QueueWaker(self._queue)
    
     