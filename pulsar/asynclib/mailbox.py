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

      from pulsar.api import send

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

.. autoclass:: MessageConsumer
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
import logging
from functools import partial
from collections import namedtuple
from inspect import isawaitable

from ..utils.exceptions import CommandError
from ..utils.internet import nice_address
from ..utils.websocket import frame_parser
from ..utils.string import gen_unique_id
from ..utils.lib import ProtocolConsumer

from .protocols import Connection
from .access import get_actor
from .proxy import actor_identity, get_proxy, get_command, ActorProxy
from .clients import AbstractClient


CommandRequest = namedtuple('CommandRequest', 'actor caller connection')
LOGGER = logging.getLogger('pulsar.mailbox')


def create_aid():
    return gen_unique_id()[:8]


async def command_in_context(command, caller, target, args, kwargs,
                             connection=None):
    cmnd = get_command(command)
    if not cmnd:
        raise CommandError('unknown %s' % command)
    request = CommandRequest(target, caller, connection)
    result = cmnd(request, args, kwargs)
    try:
        result = await result
    except TypeError:
        if isawaitable(result):
            raise
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
    """Protocol Consumer for Actor messages
    """
    tasks = None
    parser = None
    worker = None
    parser = None
    debug = False
    pending_responses = None

    def start_request(self):
        actor = get_actor()
        self.parser = frame_parser(kind=2)
        self.pending_responses = {}
        self.tasks = {}
        self.logger = actor.logger
        self.debug = actor.cfg.debug

    def feed_data(self, data):
        msg = self.parser.decode(data)
        while msg:
            try:
                message = pickle.loads(msg.body)
            except Exception:
                self.logger.exception('could not decode message body')
            else:
                # Avoid to create a task on callbacks
                if message.get('command') == 'callback':
                    self._on_callback(message)
                else:
                    task = self._loop.create_task(self._on_message(message))
                    self.tasks[message['id']] = task
            msg = self.parser.decode()

    def send(self, command, sender, target, args, kwargs):
        """Used by the server to send messages to the client.
        Returns a future.
        """
        command = get_command(command)
        data = {'command': command.__name__,
                'id': create_aid(),
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
        else:
            if not ack:
                waiter.set_result(None)
        return waiter

    def write(self, msg):
        obj = pickle.dumps(msg, protocol=2)
        data = self.parser.encode(obj, opcode=2)
        try:
            self.connection.write(data)
        except (socket.error, RuntimeError):
            actor = get_actor()
            if actor.is_running() and not actor.is_arbiter():
                self.logger.warning('Lost connection with arbiter')
                self._loop.stop()

    def _on_callback(self, message):
        ack = message.get('ack')
        if not ack:
            self.logger.error('A callback without id')
        else:
            if self.debug:
                self.logger.debug('Callback from "%s"', ack)
            pending = self.pending_responses.pop(ack)
            pending.set_result(message.get('result'))

    async def _on_message(self, message):
        try:
            actor = get_actor()
            command = message.get('command')
            ack = message.get('ack')
            try:
                if self.debug:
                    self.logger.debug('Got message "%s"', command)
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
                self.logger.warning('Command error: %s' % exc)
                result = None
            except Exception:
                self.logger.exception('Unhandled exception')
                result = None
            if ack:
                data = {'command': 'callback', 'result': result, 'ack': ack}
                self.write(data)
        finally:
            self.tasks.pop(message['id'], None)


mailbox_protocol = partial(Connection, MessageConsumer)


class MailboxClient(AbstractClient):
    """Used by actors to send messages to other actors via the arbiter.
    """
    def __init__(self, address, actor, loop):
        super().__init__(mailbox_protocol, loop=loop,
                         name='%s-mailbox' % actor, logger=LOGGER)
        self.address = address
        self.connection = None

    def connect(self):
        return self.create_connection(self.address)

    def __repr__(self):
        return '%s %s' % (self.name, nice_address(self.address))

    async def send(self, command, sender, target, args, kwargs):
        if self.connection is None:
            self.connection = await self.connect()
            consumer = self.connection.current_consumer()
            self.connection.event('connection_lost').bind(self._lost)
            consumer.start()
        else:
            consumer = self.connection.current_consumer()
        response = await consumer.send(command, sender, target, args, kwargs)
        return response

    def close(self):
        if self.connection:
            self.connection.abort()

    def start_serving(self):    # pragma    nocover
        pass

    def _lost(self, _, exc=None):
        # When the connection is lost, stop the event loop
        if self._loop.is_running():
            self._loop.stop()
