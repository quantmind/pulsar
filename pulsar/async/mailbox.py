import io
import sys
import logging
import socket
import time
import threading
from multiprocessing.queues import Empty, Queue

from pulsar import create_connection, MailboxError, server_socket,\
                    wrap_socket, CannotCallBackError, CouldNotParse
from pulsar.utils.tools import gen_unique_id
from pulsar.utils.httpurl import to_bytes

from .eventloop import IOLoop, deferred_timeout
from .defer import Deferred, make_async, safe_async, pickle, thread_loop,\
                    async, is_failure, ispy3k
from .iostream import AsyncIOStream, thread_ioloop
from .client import SocketClient


__all__ = ['PulsarClient', 'mailbox', 'Mailbox', 'IOQueue',
           'Empty', 'Queue', 'ActorMessage']


def mailbox(actor, address=None):
    '''Creates a :class:`Mailbox` instances for :class:`Actor` instances.
If an address is provided, the communication is implemented using a socket,
otherwise a queue is used.'''   
    if address:
        return PulsarClient(address=address, blocking=0)
    else:
        if actor.is_monitor():
            mailbox = MonitorMailbox(actor)
        else:
            mailbox = Mailbox(actor)
        # Add the actor to the loop tasks
        mailbox.ioloop.add_loop_task(actor)
        return mailbox


class MessageParser(object):
    
    def encode(self, msg):
        if isinstance(msg, ActorMessage):
            return msg.encode()
        else:
            return to_bytes(msg)
        
    def decode(self, buffer):
        return ActorMessage.decode(buffer)
        
    
class ActorMessage(object):
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
    
'''
    MESSAGES = {}
    
    def __init__(self, action, sender=None, receiver=None,
                 ack=True, args=(), kwargs=None):
        self.action = action
        self.sender = sender
        self.receiver = receiver
        self.args = args
        self.kwargs = kwargs
        self.ack = ack
    
    @classmethod
    def decode(cls, buffer):
        separator = b'\r\n\r\n\r\n'
        if buffer[:1] != b'*':
            raise CouldNotParse()
        idx = buffer.find(separator)
        if idx < 0:
            return None, buffer
        length = int(buffer[1:idx])
        idx += len(separator)
        total_length = idx + length
        if len(buffer) >= total_length:
            data, buffer = buffer[idx:total_length:], buffer[total_length:]
            if not ispy3k:
                data = bytes(data)
            args = pickle.loads(data)
            return cls(*args), buffer
        else:
            return None, buffer
    
    def encode(self):
        data = (self.action, self.sender, self.receiver,
                self.ack, self.args, self.kwargs)
        bdata = pickle.dumps(data)
        return ('*%s\r\n\r\n\r\n' % len(bdata)).encode('utf-8') + bdata
        
    def __repr__(self):
        return self.action
    

class PulsarClient(SocketClient):
    '''A proxy for the :attr:`Actor.inbox` attribute. It is used by the
:class:`ActorProxy` to send messages to the remote actor.'''
    parsercls = MessageParser
    
    def on_parsed_data(self, msg):
        '''Those two messages are special'''
        if msg.action == 'callback':
            return msg.args[0]
        elif msg.action == 'errback':
            raise RuntimeError()
        else:
            return msg
        
    def ping(self):
        return self.execute(ActorMessage('ping'))
    
    def run(self, callable):
        return self.execute(ActorMessage('run', args=(callable,)))
        
    def info(self):
        return self.execute(ActorMessage('info'))
    
    def shutdown(self):
        return self.execute(ActorMessage('stop'))
    

class MailboxClient(SocketClient):
    '''A :class:`MailboxClient` is a socket which receives messages
from a remote :class:`Actor`.
An instance of this class is created when a new connection is made
with a :class:`Mailbox`.'''
    parsercls = MessageParser
    def __init__(self, mailbox, sock, client_address=None):
        self.mailbox = mailbox
        self.received = 0
        self.client_address = client_address
        sock = AsyncIOStream(sock, timeout=None)
        super(MailboxClient, self).__init__(sock=sock)
        #kick off reading
        self.read()
    
    def on_read_error(self, failure):
        self.mailbox.clients.pop(self.fileno(), None)
    
    def on_parsed_data(self, msg):
        if msg:
            self.received += 1
            return self._handle_messages(msg)
            
    @async
    def _handle_messages(self, message):
        actor = self.mailbox.actor
        sender = actor.get_actor(message.sender)
        receiver = actor.get_actor(message.receiver)
        ack = message.ack
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        if not receiver:
            receiver = actor
        ack = message.ack
        #yield safe_async(receiver.on_message, args=(message,))
        func = receiver.actor_functions.get(message.action)
        args = (receiver, sender)
        if func is None:
            args += (message.action)
            func = receiver.handle_message
        args += message.args
        result = safe_async(func, args=args, kwargs=message.kwargs)
        yield result
        result = result.result
        if ack:
            if is_failure(result):
                yield self.send(ActorMessage('errback', args=(str(result),)))
            else:
                yield self.send(ActorMessage('callback', args=(result,)))
        yield result
        
    def _register_actor(self, actor):
        '''Register actor'''
        if actor:
            actor.local['mailbox'] = self
            return self.mailbox.actor.proxy
                
class MailboxInterface(object):
    address = None
    actor = None
    
    def start(self):
        pass
    
    def fileno(self):
        pass
    
    def close(self):
        pass
    
    
class Mailbox(MailboxInterface):
    '''Mailbox for an :class:`Actor`. If the actor is a
:ref:`CPU bound worker <cpubound>`, the class:`Mailbox`
creates its own :class:`IOLoop` which runs on a separate thread
of execution.

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
      
.. attribute:: clients

    A dictionary of :class:`MailboxClient` representing remote
    :class:`Actor` communicating with this :class:`Mailbox`.
'''
    thread = None
    _started = False
    def __init__(self, actor):
        self.actor = actor
        self.sock = server_socket(backlog=64)
        self.clients = {}
        self.name = '{0} mailbox {1}:{2}'.format(actor, *self.address)
        # If the actor has a ioqueue (CPU bound actor) we create a new ioloop
        if self.cpubound:
            self.__ioloop = IOLoop(pool_timeout=actor._pool_timeout,
                                   logger=actor.log,
                                   name=self.name)
        # add the on message handler for reading messages
        self.ioloop.add_handler(self,
                                self.on_connection,
                                self.ioloop.READ)
        # Kick start the mailbox in once the requestloop starts
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
        '''Called when a new connecation is available.'''
        ioloop = self.ioloop
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
        
    def unregister(self):
        if not self.ioloop.remove_loop_task(self.actor):
            self.actor.log.warn('"%s" could not be removed from eventloop'\
                                 % self.actor)
            
    def close(self):
        '''Close the mailbox. This is called by the actor when
shutting down.'''
        self.unregister()
        for c in self.clients.values():
            c.close()
        self.ioloop.remove_handler(self)
        self.sock.close()
        if self.cpubound:
            self.ioloop.stop()
            # We join the thread
            if threading.current_thread() != self.thread:
                self.thread.join()

    def start(self):
        '''Start the thread only if the mailbox has its own event loop. This
is the case when the actor is a CPU bound worker.'''
        if self.cpubound:
            if self.thread and self.thread.is_alive():
                raise RunTimeError('Cannot start mailbox. '\
                                   'It has already started')
            self.thread = threading.Thread(name=self.ioloop.name,
                                           target=self._run)
            self.thread.start()
        if not self._started:
            self._started = True
            self._register()
            # We need to register this address with the arbiter so it
            # knows how to communicate with this actor
            if not self.actor.is_arbiter():
                msg = self.actor.send('arbiter', 'mailbox_address',
                                      self.address)
                msg.add_callback(self.actor.link_actor,
                                 lambda r: r.raise_all())
            
            
    ############################################################## INTERNALS
    def _register(self):
        thread_loop(self.actor.requestloop)
        thread_ioloop(self.ioloop)
        
    def _run(self):
        self._register()
        self.ioloop.start()


class MonitorMailbox(MailboxInterface):
    '''A :class:`Mailbox` for a :class:`Monitor`. This is a proxy for the
arbiter inbox.'''
    def __init__(self, actor):
        self.actor = actor
        self.mailbox = actor.arbiter.mailbox
    
    @property
    def cpubound(self):
        return self.actor.cpubound
    
    @property
    def address(self):
        return self.mailbox.address
    
    @property
    def ioloop(self):
        return self.mailbox.ioloop
    
    def close(self):
        self.mailbox.unregister()


class QueueWaker(object):
    '''A waker for :class:`IOQueue`. Used by CPU-bound actors.'''
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
    
     