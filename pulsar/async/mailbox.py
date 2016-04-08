'''Actors communicate with each other by sending and receiving messages.
The :mod:`pulsar.async.mailbox` module implements the message passing layer
via a bidirectional socket connections between the :class:`.Arbiter`
and any :class:`.Actor`.

Message sending is asynchronous and safe, the message is guaranteed to
eventually reach the recipient, provided that the recipient exists.

The implementation details are outlined below:

* Messages are sent via the :func:`.send` function, which is a proxy for
  the actor :meth:`~.Actor.send` method.
  Here is how you ping actor ``abc`` in a coroutine::

      from pulsar import send

      async def example():
          result = await send('abc', 'ping')

* The :class:`.Arbiter` :attr:`~pulsar.Actor.mailbox` is a :class:`.TcpServer`
  accepting connections from remote actors.
* The :attr:`.Actor.mailbox` is a :class:`.MailboxClient` of the arbiter
  mailbox server.
* When an actor sends a message to another actor, the arbiter mailbox behaves
  as a proxy server by routing the message to the targeted actor.
* Communication is bidirectional and there is **only one connection** between
  the arbiter and any given actor.
* Messages are encoded and decoded using the unmasked websocket protocol
  implemented in :func:`.frame_parser`.
* If, for some reasons, the connection between an actor and the arbiter
  get broken, the actor will eventually stop running and garbaged collected.


Implementation
=========================
  For the curious this is how the internal protocol is implemented

Protocol
~~~~~~~~~~~~

.. autoclass:: MailboxProtocol
  :members:
  :member-order: bysource

Client
~~~~~~~~~~~~

.. autoclass:: MailboxClient
  :members:
  :member-order: bysource

'''
import socket
import pickle
from collections import namedtuple

from pulsar import ProtocolError, CommandError
from pulsar.utils.internet import nice_address
from pulsar.utils.websocket import frame_parser
from pulsar.utils.string import gen_unique_id

from .access import get_actor, isawaitable
from .futures import Future, task
from .proxy import actor_identity, get_proxy, get_command, ActorProxy
from .protocols import Protocol
from .clients import AbstractClient


CommandRequest = namedtuple('CommandRequest', 'actor caller connection')


def create_aid():
    return gen_unique_id()[:8]


async def command_in_context(command, caller, target, args, kwargs,
                             connection=None):
    cmnd = get_command(command)
    if not cmnd:
        raise CommandError('unknown %s' % command)
    request = CommandRequest(target, caller, connection)
    result = cmnd(request, args, kwargs)
    if isawaitable(result):
        result = await result
    return result


class ProxyMailbox:
    '''A proxy for the arbiter :class:`Mailbox`.
    '''
    active_connections = 0

    def __init__(self, actor):
        mailbox = actor.monitor.mailbox
        if isinstance(mailbox, ProxyMailbox):
            mailbox = mailbox.mailbox
        self.mailbox = mailbox

    def __repr__(self):
        return self.mailbox.__repr__()

    def __str__(self):
        return self.mailbox.__str__()

    def __getattr__(self, name):
        return getattr(self.mailbox, name)

    def _run(self):
        pass

    def close(self):
        pass


class Message:
    '''A message which travels from actor to actor.
    '''
    def __init__(self, data, waiter=None):
        self.data = data
        self.waiter = waiter

    def __repr__(self):
        return self.data.get('command', 'unknown')
    __str__ = __repr__

    @classmethod
    def command(cls, command, sender, target, args, kwargs):
        command = get_command(command)
        data = {'command': command.__name__,
                'sender': actor_identity(sender),
                'target': actor_identity(target),
                'args': args if args is not None else (),
                'kwargs': kwargs if kwargs is not None else {}}
        waiter = Future()
        if command.ack:
            data['ack'] = create_aid()
        else:
            waiter.set_result(None)
        return cls(data, waiter)

    @classmethod
    def callback(cls, result, ack):
        data = {'command': 'callback', 'result': result, 'ack': ack}
        return cls(data)


class MailboxProtocol(Protocol):
    '''The :class:`.Protocol` for internal message passing between actors.

    Encoding and decoding uses the unmasked websocket protocol.
    '''
    def __init__(self, **kw):
        super().__init__(**kw)
        self._pending_responses = {}
        self._parser = frame_parser(kind=2, pyparser=True)
        actor = get_actor()
        if actor.is_arbiter():
            self.bind_event('connection_lost', self._connection_lost)

    def request(self, command, sender, target, args, kwargs):
        '''Used by the server to send messages to the client.'''
        req = Message.command(command, sender, target, args, kwargs)
        self._start(req)
        return req.waiter

    def data_received(self, data):
        # Feed data into the parser
        msg = self._parser.decode(data)
        while msg:
            try:
                message = pickle.loads(msg.body)
            except Exception as e:
                raise ProtocolError('Could not decode message body: %s' % e)
            self._on_message(message)
            msg = self._parser.decode()

    ########################################################################
    #    INTERNALS
    def _start(self, req):
        if req.waiter and 'ack' in req.data:
            self._pending_responses[req.data['ack']] = req.waiter
            try:
                self._write(req)
            except Exception as exc:
                req.waiter.set_exception(exc)
        else:
            self._write(req)

    def _connection_lost(self, _, exc=None):
        if exc:
            actor = get_actor()
            if actor.is_running():
                actor.logger.warning('Connection lost with actor.')

    @task
    async def _on_message(self, message):
        actor = get_actor()
        command = message.get('command')
        ack = message.get('ack')
        if command == 'callback':
            if not ack:
                raise ProtocolError('A callback without id')
            try:
                pending = self._pending_responses.pop(ack)
            except KeyError:
                raise KeyError('Callback %s not in pending callbacks' % ack)
            pending.set_result(message.get('result'))
        else:
            try:
                target = actor.get_actor(message['target'])
                if target is None:
                    raise CommandError('cannot execute "%s", unknown actor '
                                       '"%s"' % (command, message['target']))
                # Get the caller proxy without throwing
                caller = get_proxy(actor.get_actor(message['sender']),
                                   safe=True)
                if isinstance(target, ActorProxy):
                    # route the message to the actor proxy
                    if caller is None:
                        raise CommandError(
                            "'%s' got message from unknown '%s'"
                            % (actor, message['sender']))
                    result = await actor.send(target, command,
                                              *message['args'],
                                              **message['kwargs'])
                else:
                    result = await command_in_context(command, caller, target,
                                                      message['args'],
                                                      message['kwargs'],
                                                      self)
            except CommandError as exc:
                self.logger.warning('Command error: %s' % exc)
                result = None
            except Exception as exc:
                self.logger.exception('Unhandled exception')
                result = None
            if ack:
                self._start(Message.callback(result, ack))

    def _write(self, req):
        obj = pickle.dumps(req.data, protocol=2)
        data = self._parser.encode(obj, opcode=2)
        try:
            self._transport.write(data)
        except socket.error:
            actor = get_actor()
            if actor.is_running():
                if actor.is_arbiter():
                    raise
                else:
                    actor.logger.warning('Lost connection with arbiter')
                    actor._loop.stop()


class MailboxClient(AbstractClient):
    '''Used by actors to send messages to other actors via the arbiter.
    '''
    protocol_factory = MailboxProtocol

    def __init__(self, address, actor, loop):
        super().__init__(loop)
        self.address = address
        self.name = 'Mailbox for %s' % actor
        self._connection = None

    def response(self, request):
        resp = super().response
        self._consumer = resp(request, self._consumer, False)
        return self._consumer

    def connect(self):
        return self.create_connection(self.address)

    def __repr__(self):
        return '%s %s' % (self.name, nice_address(self.address))

    @task
    async def request(self, command, sender, target, args, kwargs):
        # the request method
        if self._connection is None:
            self._connection = await self.connect()
            self._connection.bind_event('connection_lost', self._lost)
        req = Message.command(command, sender, target, args, kwargs)
        self._connection._start(req)
        response = await req.waiter
        return response

    def start_serving(self):
        pass

    def close(self):
        if self._connection:
            self._connection.close()

    def _lost(self, _, exc=None):
        # When the connection is lost, stop the event loop
        if self._loop.is_running():
            self._loop.stop()
