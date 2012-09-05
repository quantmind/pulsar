import io
import sys
import logging
import socket
import time
import threading
from multiprocessing.queues import Empty, Queue

from pulsar import create_connection, MailboxError, server_socket,\
                    wrap_socket, CouldNotParse, CommandNotFound,\
                    defaults
from pulsar.utils.httpurl import to_bytes

from .defer import maybe_async, pickle, is_async, log_failure,\
                    async, is_failure, ispy3k, raise_failure, CLEAR_ERRORS
from .iostream import AsyncIOStream, AsyncSocketServer,\
                        AsyncConnection, ReconnectingClient, AsyncResponse
from .access import get_actor


__all__ = ['PulsarClient', 'mailbox', 'Mailbox', 'IOQueue',
           'Empty', 'Queue', 'ActorMessage']


def mailbox(actor=None, address=None):
    '''Creates a :class:`Mailbox` instances for :class:`Actor` instances.
If an address is provided, the communication is implemented using a socket,
otherwise a queue is used.'''
    if address:
        return PulsarClient.connect(address, timeout=0)
    else:
        if actor.is_monitor():
            return MonitorMailbox(actor)
        else:
            return Mailbox.make(actor).start()

def actorid(actor):
    return actor.aid if hasattr(actor,'aid') else actor


class MessageParser(object):

    def encode(self, msg):
        if isinstance(msg, ActorMessage):
            return msg.encode()
        else:
            return to_bytes(msg)

    def decode(self, buffer):
        return ActorMessage.decode(buffer)


class ActorMessage(object):
    '''A message which travels from :class:`Actor` to
:class:`Actor` to perform a specific *command*. :class:`ActorMessage`
are not directly initialised using the constructor, instead they are
created by :meth:`ActorProxy.send` method.

.. attribute:: sender

    id of the actor sending the message.

.. attribute:: receiver

    id of the actor receiving the message.

.. attribute:: command

    command to be performed

.. attribute:: args

    Positional arguments in the message body

.. attribute:: kwargs

    Optional arguments in the message body
'''
    def __init__(self, command, sender=None, receiver=None,
                 args=None, kwargs=None):
        self.command = command
        self.sender = actorid(sender)
        self.receiver = actorid(receiver)
        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}

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
        data = (self.command, self.sender, self.receiver,
                self.args, self.kwargs)
        bdata = pickle.dumps(data, protocol=2)
        return ('*%s\r\n\r\n\r\n' % len(bdata)).encode('utf-8') + bdata

    def __repr__(self):
        return self.command


class PulsarClient(ReconnectingClient):
    '''A proxy for the :attr:`Actor.inbox` attribute. It is used by the
:class:`ActorProxy` to send messages to the remote actor.'''
    parser_class = MessageParser

    def parsedata(self, data):
        msg = super(PulsarClient, self).parsedata(data)
        if msg:
            # Those two messages are special
            if msg.command in ('callback', 'errback'):
                return msg.args[0] or ''
            else:
                return msg

    @raise_failure
    def ping(self):
        return self.execute(ActorMessage('ping'))

    @raise_failure
    def echo(self, message):
        return self.execute(ActorMessage('echo', args=(message,)))

    @raise_failure
    def run_code(self, code):
        return self.execute(ActorMessage('run_code', args=(code,)))

    @raise_failure
    def info(self):
        return self.execute(ActorMessage('info'))

    @raise_failure
    def quit(self):
        return self.execute(ActorMessage('quit'))

    @raise_failure
    def shutdown(self):
        return self.execute(ActorMessage('stop'))


class MailboxResponse(AsyncResponse):
    __slots__ = ('connection', 'parsed_data')
    def __iter__(self):
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        message = self.parsed_data
        actor = self.connection.actor
        receiver = actor.get_actor(message.receiver) or actor
        sender = receiver.get_actor(message.sender)
        command = receiver.command(message.command)
        try:
            if not command:
                raise CommandNotFound(message.command)
            args = message.args
            # If this is an internal command add the sender information
            if command.internal:
                args = (sender,) + args
            result = command(self, receiver, *args, **message.kwargs)
        except:
            result = sys.exc_info()
        result = maybe_async(result)
        while is_async(result):
            yield b''
            result = maybe_async(result)
        log_failure(result)
        if command.ack:
            # Send back the result as an ActorMessage
            if is_failure(result):
                m = ActorMessage('errback', sender=receiver, args=(result,))
            else:
                m = ActorMessage('callback', sender=receiver, args=(result,))
            yield self.parser.encode(m)


class MailboxConnection(AsyncConnection):
    '''A :class:`MailboxClient` is a socket which receives messages
from a remote :class:`Actor`.
An instance of this class is created when a new connection is made
with a :class:`Mailbox`.'''
    authenticated = False
    response_class = MailboxResponse


class Mailbox(AsyncSocketServer):
    '''Mailbox for an :class:`Actor`. If the actor is a
:ref:`CPU bound worker <cpubound>`, the class:`Mailbox`
creates its own :class:`IOLoop` which runs on a separate thread
of execution.'''
    parser_class = MessageParser
    connection_class = MailboxConnection

    @classmethod
    def make(cls, actor, backlog=64):
        return super(Mailbox, cls).make(actor=actor, backlog=backlog,
                                        onthread=actor.cpubound,
                                        timeout=None)

    @property
    def name(self):
        return '%s mailbox %s:%s' %\
             (self.actor, self.address[0], self.address[1])

    @property
    def cpubound(self):
        return self.actor.cpubound

    def actor_ioloop(self):
        return self.actor.requestloop
    
    def shut_down(self):
        self.unregister(self.actor)

    def on_start(self):
        actor = self.actor
        self.register(actor)
        if not actor.is_arbiter():
            # The actor is not the arbiter. We need to register this mailbox
            # with the actor monitor so that it can receive messages from it
            return actor.send(actor.monitor, 'mailbox_address', self.address)\
                        .add_callback(actor.link_actor)

    def register(self, actor):
        self.ioloop.add_loop_task(actor)

    def unregister(self, actor):
        if not self.ioloop.remove_loop_task(actor):
            self.actor.log.warn('"%s" could not be removed from eventloop'\
                                 % actor)

class MonitorMailbox(object):
    '''A :class:`Mailbox` for a :class:`Monitor`. This is a proxy for the
arbiter inbox.'''
    active_connections = 0
    def __init__(self, actor):
        self.actor = actor
        self.mailbox = actor.arbiter.mailbox
        self.mailbox.register(self.actor)

    @property
    def cpubound(self):
        return self.actor.cpubound

    @property
    def address(self):
        return self.mailbox.address

    @property
    def ioloop(self):
        return self.mailbox.ioloop

    def start(self):
        pass

    def close(self):
        self.mailbox.unregister(self.actor)


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
    '''Epoll like class for a IO based on queues rather than sockets.
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

    def register(self, fd, events=None):
        '''Register a fd descriptor with the io queue object'''
        self._fds.add(fd)

    def modify(self, fd, events=None):
        '''Modify a registered file descriptor'''
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        '''Remove a registered file descriptor from the ioqueue object.. '''
        self._fds.discard(fd)

    def poll(self, timeout=0.5):
        '''Wait for events. timeout in seconds (float)'''
        if self._actor:
            if not self._actor.can_poll():
                return self._empty
        try:
            event = self._queue.get(timeout=timeout)
        except (Empty,IOError,TypeError,EOFError):
            return self._empty
        return (event,)

    def waker(self):
        return QueueWaker(self._queue)

