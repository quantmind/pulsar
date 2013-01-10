import sys
import logging
import tempfile

from pulsar import create_connection, MailboxError, server_socket,\
                    wrap_socket, CouldNotParse, CommandNotFound,\
                    platform, defaults
from pulsar.utils.httpurl import to_bytes

from .defer import maybe_async, pickle, is_async, log_failure,\
                    async, is_failure, ispy3k, raise_failure
from .access import get_actor
from .servers import create_server
from .protocols import Protocol, ProtocolResponse


__all__ = ['PulsarClient', 'mailbox', 'ActorMessage']


LOGGER = logging.getLogger('pulsar.mailbox')


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
            return create_mailbox(actor)

def actorid(actor):
    return actor.aid if hasattr(actor, 'aid') else actor


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
        separator = b'\r\n'
        if buffer[0] != 42:
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
        return ('*%s\r\n' % len(bdata)).encode('utf-8') + bdata

    def __repr__(self):
        return self.command

ReconnectingClient = object
class PulsarClient(ReconnectingClient):
    '''A proxy for the :attr:`Actor.inbox` attribute. It is used by the
:class:`ActorProxy` to send messages to the remote actor.'''
    protocol_factory = MessageParser

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


class MailboxResponse(ProtocolResponse):
    message = None
    def feed(self, data):
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        message, data = ActorMessage.decode(data)
        if message:
            self.message = message
            actor = get_actor()
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
            result = make_async(result).add_both(self.encode)
            self.write(result)
            
    def encode(self, result):
        log_failure(result)
        if command.ack:
            # Send back the result as an ActorMessage
            if is_failure(result):
                m = ActorMessage('errback', sender=receiver, args=(result,))
            else:
                m = ActorMessage('callback', sender=receiver, args=(result,))
            return m.encode()
        else:
            return b''

    def finished(self):
        return bool(self.message)


class MailboxProtocol(Protocol):
    '''A :class:`MailboxClient` is a socket which receives messages
from a remote :class:`Actor`.
An instance of this class is created when a new connection is made
with a :class:`Mailbox`.'''
    authenticated = False


def create_mailbox(actor):
    if platform.type == 'posix':
        address = 'unix:%s.pulsar' % actor.aid
    else:   #pragma    nocover
        address = ('127.0.0.1', 0)
    return create_server(actor, address=address, onthread=actor.cpubound,
                         protocol=MailboxProtocol, response=MailboxResponse,
                         call_soon=send_mailbox_address)
    
def send_mailbox_address(self):
    actor = self.actor
    return actor.send(actor.monitor, 'mailbox_address', self.address)\
                     .add_callback(actor.link_actor)


class MonitorMailbox(object):
    '''A :class:`Mailbox` for a :class:`Monitor`. This is a proxy for the
arbiter inbox.'''
    active_connections = 0
    def __init__(self, actor):
        self.mailbox = actor.arbiter.mailbox

    @property
    def address(self):
        return self.mailbox.address

    @property
    def ioloop(self):
        return self.mailbox.ioloop

    def _run(self):
        pass
    
    def close(self):
        pass

