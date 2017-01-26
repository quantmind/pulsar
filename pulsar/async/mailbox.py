"""Actors communicate with each other by sending and receiving messages.
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

"""
import socket
import pickle
import asyncio
from collections import namedtuple

from ..utils.exceptions import ProtocolError, CommandError
from ..utils.internet import nice_address
from ..utils.websocket import frame_parser
from ..utils.string import gen_unique_id
from ..utils.lib import ProtocolConsumer

from .protocols import PulsarProtocol
from .access import get_actor, isawaitable
from .proxy import actor_identity, get_proxy, get_command, ActorProxy
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


class MessageConsumer(ProtocolConsumer):
    messages = None
    parser = None
    worker = None

    def start_request(self):
        self.messages = asyncio.Queue(loop=self._loop)
        self.worker = self._loop.create_task(self._process_messages())
        self.event('post_request').bind(self._close_worker)

    def feed_data(self, data):
        msg = self.connection.parser.decode(data)
        if msg:
            try:
                message = pickle.loads(msg.body)
            except Exception:
                self.producer.logger.exception(
                    'could not decode message body'
                )
            else:
                self.messages.put_nowait(message)

    async def _process_messages(self):
        actor = get_actor()
        logger = self.producer.logger
        while actor.is_running():
            message = await self.messages.get()
            command = message.get('command')
            ack = message.get('ack')
            try:
                logger.debug('Got message %s', command)
                if command == 'callback':
                    if not ack:
                        raise ProtocolError('A callback without id')
                    pending = self.connection.pending_responses.pop(ack)
                    pending.set_result(message.get('result'))
                else:
                    target = actor.get_actor(message['target'])
                    if target is None:
                        raise CommandError(
                            'cannot execute "%s", unknown actor '
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
                        result = await command_in_context(command, caller,
                                                          target,
                                                          message['args'],
                                                          message['kwargs'],
                                                          self)
            except CommandError as exc:
                logger.warning('Command error: %s' % exc)
                result = None
            except Exception as exc:
                logger.exception('Unhandled exception')
                result = None
            if ack:
                data = {'command': 'callback', 'result': result, 'ack': ack}
                self.connection.write(data)

    def _close_worker(self, _, **kw):
        if self.worker and not self.worker.done():
            self.worker.cancel()
            self.worker = None


class MailboxConnection(PulsarProtocol):
    """The :class:`.Protocol` for internal message passing between actors.

    Encoding and decoding uses the unmasked websocket protocol.
    """
    pending_responses = None
    parser = None

    @classmethod
    def create(cls, producer):
        connection = cls(MessageConsumer, producer)
        connection.pending_responses = {}
        connection.parser = frame_parser(kind=2)
        return connection

    def request(self, command, sender, target, args, kwargs):
        """Used by the server to send messages to the client.
        """
        command = get_command(command)
        data = {'command': command.__name__,
                'sender': actor_identity(sender),
                'target': actor_identity(target),
                'args': args if args is not None else (),
                'kwargs': kwargs if kwargs is not None else {}}

        waiter = self._loop.create_future()
        ack = None
        if command.ack:
            ack = create_aid()
            data['ack'] = ack
            self.pending_responses[ack] = waiter

        try:
            self.write(data)
        except Exception as exc:
            waiter.set_exception(exc)
            if ack:
                self.pending_responses.pop(ack, None)

        return waiter

    def write(self, msg):
        obj = pickle.dumps(msg, protocol=2)
        data = self.parser.encode(obj, opcode=2)
        try:
            self.transport.write(data)
        except (socket.error, RuntimeError):
            actor = get_actor()
            if actor.is_running() and not actor.is_arbiter():
                actor.logger.warning('Lost connection with arbiter')
                actor._loop.stop()


class MailboxClient(AbstractClient):
    """Used by actors to send messages to other actors via the arbiter.
    """
    def __init__(self, address, actor, loop):
        super().__init__(MailboxConnection.create, loop=loop,
                         name='%s-mailbox' % actor)
        self.address = address
        self._connection = None

    def connect(self):
        return self.create_connection(self.address)

    def __repr__(self):
        return '%s %s' % (self.name, nice_address(self.address))

    async def request(self, command, sender, target, args, kwargs):
        # the request method
        if self._connection is None:
            self._connection = await self.connect()
            self._connection.event('connection_lost').bind(self._lost)
        response = await self._connection.request(
            command, sender, target, args, kwargs
        )
        return response

    def close(self):
        if self._connection:
            self._connection.close()

    def start_serving(self):    # pragma    nocover
        pass

    def _lost(self, _, exc=None):
        # When the connection is lost, stop the event loop
        if self._loop.is_running():
            self._loop.stop()
