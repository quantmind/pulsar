import io
import sys
import logging
import socket
import time
import threading
from multiprocessing.queues import Empty, Queue

from pulsar import create_connection, MailboxError, socket_pair, wrap_socket,\
                    CannotCallBackError
from pulsar.utils.tools import gen_unique_id

from .eventloop import IOLoop, deferred_timeout
from .defer import Deferred, make_async, safe_async, pickle, thread_loop,\
                    is_failure, ispy3k
from .iostream import AsyncIOStream, thread_ioloop


__all__ = ['mailbox', 'Mailbox', 'IOQueue', 'Empty', 'Queue', 'ActorMessage']


def mailbox(actor, address=None):
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


def serverSocket():
    '''Create the inbox, a TCP socket ready for
accepting messages from other actors.'''
    # get a socket pair
    w, s = socket_pair(backlog = 64)
    s.setblocking(True)
    r, _ = s.accept()
    r.close()
    w.close()
    s.setblocking(False)
    return s
        

def encode_message(msg):
    data = (msg.rid,msg.sender,msg.receiver,msg.action,
            msg.ack,msg.args,msg.kwargs)
    bdata = pickle.dumps(data)
    return ('*%s\r\n\r\n\r\n' % len(bdata)).encode('utf-8') + bdata
    
def decode_message(buffer):
    if not buffer:
        return None, buffer
    if buffer[:1] != b'*':
        raise ValueError('Could not parse message')
    separator = b'\r\n\r\n\r\n'
    idx = buffer.find(separator)
    if idx < 0:
        return None, buffer
    length = int(buffer[1:idx])
    idx += len(separator)
    total_length = idx + length
    msg = None
    if len(buffer) >= total_length:
        data, buffer = buffer[idx:total_length:], buffer[total_length:]
        if not ispy3k:
            data = bytes(data)
        args = pickle.loads(data)
        msg = ActorMessage(*args)
    return msg, buffer
    
    
class ActorMessage(Deferred):
    '''A message class which travels from :class:`Actor` to
:class:`Actor` to perform a specific *action*. :class:`ActorMessage`
are not directly initialized using the constructor, instead they are
created by :meth:`ActorProxy.send` method.

.. attribute:: sender

    id of the actor sending the message.
    
.. attribute:: receiver

    id of the actor receiving the message.
    
.. attribute:: action

    action to be performed
    
.. attribute:: args

    Positional arguments in the message body
    
.. attribute:: kwargs

    Optional arguments in the message body
    
.. attribute:: ack

    ``True`` if the message needs acknowledgment
    
.. attribute:: rid

    :class:`ActorMessage` request id. This id used for managing remote
    callbacks, if :attr:`ack` is ``True``.
'''
    MESSAGES = {}
    
    def __init__(self, rid, sender, target, action, ack, args, kwargs):
        super(ActorMessage,self).__init__()
        if rid is None:
            rid = gen_unique_id()[:8]
            if ack:
                self.MESSAGES[rid] = self
        self.rid = rid
        self.sender = sender
        self.receiver = target
        self.action = action
        self.args = args
        self.kwargs = kwargs
        self.ack = ack
        
    def __repr__(self):
        return '%s %s %s' % (self.sender, self.action, self.receiver)
        
    def add_callback(self, callback, errback=None):
        if not self.ack:
            raise CannotCallBackError('Cannot add callback to "{0}".\
 It does not acknowledge'.format(self))
        return super(ActorMessage, self).add_callback(callback, errback)
    
    @classmethod
    def actor_callback(cls, rid, result):
        r = cls.MESSAGES.pop(rid, None)
        if r:
            r.callback(result)
            
            
class MailboxProxy(object):
    '''A socket outbox for :class:`Actor` instances. This outbox
send messages to a :class:`SocketServerMailbox`.'''
    def __init__(self, address):
        self.stream = AsyncIOStream()
        self.stream.connect(address)
    
    @property
    def address(self):
        return self.stream.getsockname()
    
    def fileno(self):
        return self.stream.fileno()
        
    def put(self, msg):
        data = encode_message(msg)
        self.stream.write(data)
    
    def close(self):
        self.stream.close()
        

class MailboxClient(object):
    '''A :class:`MailboxClient` is a read only socket which
receives messages from a remote :class:`Actor`.
An instance of this class is created when a new connection is made
with a :class:`Mailbox`.'''
    def __init__(self, mailbox, sock, client_address):
        self.mailbox = mailbox
        self.received = 0
        # create an asyncronous iostream without timeout
        self.stream = AsyncIOStream(sock, timeout=None)
        self.client_address = client_address
        self.buffer = bytearray()
        #kick off reading
        self.read()
        
    def fileno(self):
        return self.stream.fileno()
    
    def __str__(self):
        return '%s' % self.stream
        
    def read(self):
        self.stream.read().add_callback(self._parse, self._error)
    
    def _error(self, failure):
        failure.log()
        self.mailbox.clients.pop(self.fileno(), None)
        self.stream.close()
    
    def _parse(self, data):
        buffer = self.buffer
        if data:
            buffer.extend(data)
        msg = True
        while msg is not None:
            msg, buffer = decode_message(buffer)
            if msg is not None:
                self.received += 1
                self.mailbox.message_arrived(msg)
        self.buffer = buffer
        self.read()
    
    
class Mailbox(object):
    '''A socket mailbox for :class:`Actor` instances. If the *actor* is a
CPU bound worker (an actor which communicate with its monitor via a message
queue), the class:`Mailbox` will create its own :class:`IOLoop`.

A :class:`Mailbox` is created during an :class:`Actor` startup, in the
actor process domain.

.. attribute:: actor

    The :class:`Actor` which uses this :class:`Mailbox` to send and receive
    :class:`ActorMessage`.
    
.. attribute:: sock

    The socket which is initialised during construction with a random local
    address.
    
.. attribute:: ioloop

    The :class:`IOLoop` used by this mailbox for asynchronously sending and
    receiving :class:`ActorMessage`. There are two possibilities:
    
    * The mailbox shares the eventloop with the :attr:`actor`. This is the
      most common case, when the actor is not a CPU bound worker.
    * The mailbox has its own eventloop. This is the case when the :attr:`actor`
      is a CPU-bound worker, and uses its main event loop for communicating
      with a queue rather than with a socket.
'''
    thread = None
    _started = False
    def __init__(self, actor):
        self.actor = actor
        self.sock = serverSocket()
        self.pending = {}
        self.clients = {}
        self.name = '{0} mailbox {1}:{2}'.format(actor,*self.address)
        # If the actor has a ioqueue (CPU bound actor) we create a new ioloop
        if self.cpubound:
            self.__ioloop = IOLoop(pool_timeout=actor._pool_timeout,
                                   logger=actor.log,
                                   name=self.name)
        # add the on message handler for reading messages
        self.ioloop.add_handler(self,
                                self.on_connection,
                                self.ioloop.READ)
        actor.requestloop.add_callback(self.start)
    
    def __repr__(self):
        return self.name
    __str__ = __repr__
    
    @property
    def cpubound(self):
        return self.actor.cpubound
    
    @property
    def ioloop(self):
        return self.__ioloop if self.cpubound else self.actor.requestloop
    
    @property
    def address(self):
        return self.sock.getsockname()
    
    def fileno(self):
        return self.sock.fileno()
        
    def on_connection(self, fd, events):
        '''Called when a new message has arrived.'''
        ioloop = self.actor.ioloop
        client = self.clients.get(fd)
        if not client:
            client, client_address = self.sock.accept()
            if not client:
                self.actor.log.debug('Still no client. Aborting')
            else:
                client = MailboxClient(self, client, client_address)
                self.clients[client.fileno()] = client
        else:
            self.actor.log.warn('This is a connection callback only!')
        
    def unregister(self, actor):
        if not self.ioloop.remove_loop_task(actor):
            actor.log.warn('"{0}" could not be removed from\
 eventloop'.format(self))
            
    def close(self):
        self.unregister(self.actor)
        if self.__hasloop:
            self.ioloop.log.debug('Stop event loop for {0}'.format(self))
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

    def start(self):
        '''Start the thread only if the mailbox has its own event loop. This
is the case when the actor is a CPU bound worker.'''
        if not self._started:
            self._started = True
            self._run()
        if self.cpubound:
            if self.thread and self.thread.is_alive():
                raise RunTimeError('Cannot start mailbox. '\
                                   'It has already started')
            self.thread = threading.Thread(name=self.ioloop.name,
                                           target=self._run)
            self.thread.start()
            
    def message_arrived(self, message):
        '''A new :class:`ActorMessage` has arrived in the :attr:`Actor.inbox`.
Here we check the sender and the receiver (it may be not ``self`` if
``self`` is the  :class:`Arbiter`) and perform the message action.
If the message needs acknowledgment, send the result back.'''
        return make_async(self._handle_message(message))
    
    ############################################################## INTERNALS
    def _handle_message(self, message):
        actor = self.actor
        sender = actor.get_actor(message.sender)
        receiver = actor.get_actor(message.receiver)
        ack = message.ack
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        if not receiver:
            actor.log.warn('message "%s" for an unknown actor "%s"' %
                              (message, message.receiver))
            raise StopIteration()
        
        ack = message.ack
        yield safe_async(receiver.on_message, args=(message,))
        func = receiver.actor_functions.get(message.action, None)
        if func:
            ack = getattr(func, 'ack', True)
            result = safe_async(func,
                                args=(receiver, sender) + message.args,
                                kwargs=message.kwargs)
        else:
            result = safe_async(receiver.handle_message,
                                args=(sender, message))
        yield result
        result = result.result
        if is_failure(result):
            result.log()
            raise StopIteration()
        yield safe_async(receiver.on_message_processed, args=(message, result))
        if ack:
            if sender:
                # Acknowledge the sender with the result.
                yield receiver.send(sender, 'callback', message.rid, result)
            else:
                receiver.log.error('message "{0}" from an unknown actor '\
                                   '"{1}". Cannot acknowledge.'\
                                   .format(message,message.sender))
        
    def _run(self):
        thread_loop(self.actor.requestloop)
        thread_ioloop(self.ioloop)
        if self.thread is not None:
            self.ioloop.start()
        elif not self.actor.is_arbiter():
            # We need to register this actor mailbox with the arbiter
            msg = self.actor.send('arbiter', 'mailbox_address', self.address)
            msg.add_callback(self.actor.link_actor,
                             lambda r: r.raise_all())


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
    cpubound = True
    def __init__(self, queue, actor=None):
        self._queue = queue
        self._actor = actor
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
        if self._actor:
            if not self._actor.can_poll():
                return self._empty
        try:
            event = self._queue.get(timeout = timeout)
        except (Empty,IOError,TypeError,EOFError):
            return self._empty
        return (event,)
    
    def waker(self):
        return QueueWaker(self._queue)
    
     