'''Pulsar inter-actor message pasing.
It uses bidirectional socket connections between the :class:`Arbiter`
and actors. It uses the unmasked websocket protocol.'''
import sys
import logging
import tempfile
from functools import partial

from pulsar import platform, PulsarException, Config
from pulsar.utils.pep import to_bytes, ispy3k, ispy3k, pickle, set_event_loop, get_event_loop
from pulsar.utils.websocket import FrameParser
from pulsar.utils.security import gen_unique_id

from .access import get_actor, set_actor
from .defer import make_async, log_failure, is_failure
from .servers import create_server, ServerConnection
from .protocols import ProtocolConsumer
from .proxy import actorid, CommandNotFound
from . import clients


__all__ = ['CommandNotFound']


LOGGER = logging.getLogger('pulsar.mailbox')


def load_message(data):
    return pickle.loads(data)
    

class MailboxRequest(clients.Request):
    # Actor message request
    def __init__(self, address, timeout, command, sender, receiver,
                 args, kwargs, ack, parser):
        super(MailboxRequest, self).__init__(address, timeout)
        self.parser = parser
        self.data = {'command': command,
                     'sender': actorid(sender),
                     'receiver': actorid(receiver),
                     'args': args if args is not None else (),
                     'kwargs': kwargs if kwargs is not None else {}}
        if ack:
            self.data['ack'] = gen_unique_id()[:8]
    
    @property
    def ack(self):
        return self.data.get('ack')
    
    def encode(self): 
        return dump_data(self.parser, self.data)

    def __repr__(self):
        return self.data['command']
    __str__ = __repr__
    
        
class MailboxMixin(object):
    
    def __new__(self):
        self._pending_responses = {}
        self._parser = FrameParser(kind=2)
        
    def callback(self, ack, result):
        if not ack:
            raise ProtocolError('A callback without id')
        try:
            pending = self._pending_responses.pop(ack)
        except KeyError:
            raise KeyError('Callback %s not in pending callbacks' % ack)
        pending.callback(result)
        
    def responde(self, frame):
        actor = get_actor()
        self.actor = actor.get_actor(message['receiver']) or actor
        self.caller = self.actor.get_actor(message['sender'])
        try:
            command = message['command']
            if command == 'callback':
                self.callback(message.get('ack'), message.get('result'))
            else:
                command = self.actor.command(command)
            if not command:
                raise CommandNotFound(message['command'])
            result = command(self, message['args'], message['kwargs'])
        except:
            result = sys.exc_info()
        result = make_async(result).add_both(partial(self.write, message))
        
    def write(self, msg, result):
        if is_failure(result):
            log_failure(result)
        if message.get('ack'):
            msg['result'] = result
            msg['receiver'], msg['sender'] = msg['sender'], msg['receiver']
            msg['command'] = 'callback'
            self.transport.write(self.dump_data(msg))
            
    def dump_data(self, obj):
        obj = pickle.dumps(obj, protocol=2)
        return self.parser.encode(msg).msg


class MonitorMailbox(object):
    '''A :class:`Mailbox` for a :class:`Monitor`. This is a proxy for the
arbiter inbox.'''
    active_connections = 0
    def __init__(self, actor):
        self.mailbox = actor.arbiter.mailbox
        # make sure the monitor get the hand shake!
        self.mailbox.event_loop.call_soon_threadsafe(actor.hand_shake)

    def __repr__(self):
        return self.mailbox.__repr__()
    
    def __str__(self):
        return self.mailbox.__str__()
    
    @property
    def address(self):
        return self.mailbox.address

    @property
    def event_loop(self):
        return self.mailbox.event_loop

    def _run(self):
        pass
    
    def close(self):
        pass


################################################################################
##    Mailbox Server Classes
class MailboxConnection(ServerConnection):
    authenticated = False
    
    
class MailboxResponse(ProtocolConsumer):
    
    def __init__(self, connection):
        super(MailboxResponse, self).__init__(connection)
        self.parser = FrameParser(kind=2)
        
    def feed(self, data):
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        msg = self.parser.decode(data)
        if msg:
            message = load_message(msg.body)
            self.responde(message)
    
    def encode(self, code, data=None): 
        msg = dump_data((code, data))
        frame = self.parser.encode(msg)
        return frame.msg
    
    def responde(self, frame):
        message = self.parser.
        actor = get_actor()
        self.actor = actor.get_actor(message['receiver']) or actor
        self.caller = self.actor.get_actor(message['sender'])
        command = self.actor.command(message['command'])
        try:
            if not command:
                raise CommandNotFound(message['command'])
            result = command(self, message['args'], message['kwargs'])
        except:
            result = sys.exc_info()
        result = make_async(result).add_both(partial(self.write, command.ack))
        
    def write(self, ack, result):
        if is_failure(result):
            log_failure(result)
            if ack:
                msg = self.encode('errback', result)
                self.transport.write(msg)
        elif ack:
            msg = self.encode('callback', result)
            self.transport.write(msg)
    

################################################################################
##    Mailbox Client Classes
class MailboxClientConsumer(MailboxMixin, clients.ClientProtocolConsumer):
    '''The Protocol consumer for a Mailbox client'''
    def feed(self, data):
        frame = self.request.parser.decode(data)
        if frame:
            result = load_message(frame.body)
            if result.get('')
            try:
                if code == 'errback':
                    raise PulsarException
            except:
                result = sys.exc_info()
            self.result = result
            self.finished(result)
    
    def send(self, *args):
        ack = self.request.ack
        if ack:
            self._to_acknowledge[ack] = Message(ack)
        self.write(self.request.encode())
    
    
class PulsarClient(pulsar.Client):
    # Pulsar arbiter client
    response_factory = MailboxClientConsumer
     
    def __init__(self, address, actor):
        super(PulsarClient, self).__init__(max_connections=1)
        self.address = address
        self.name = 'Mailbox for %s' % actor
        eventloop = loop = eventloop or get_event_loop()
        # The eventloop is cpubound
        if getattr(eventloop, 'cpubound', False):
            loop = get_event_loop()
            if loop is None:
                # No IO event loop available in the current thread.
                # Create one and set it as the event loop
                loop = new_event_loop()
                set_event_loop(loop)
                eventloop.call_soon_threadsafe(_start_on_thread)
        self._event_loop = loop
        
    def _start_on_thread(self):
        PulsarThread(name=self.name, target=self._event_loop.run).start()
        
    @property
    def event_loop(self):
        return self._event_loop
        
    def connect(self):
        request = clients.Request(self.address, self.timeout)
        self.consumer = self.response(request)
        
    def request(self, cmd, sender, target, args, kwargs, consumer):
        req = Request(self.address, self.timeout, cmd.__name__, sender,
                      target, args, kwargs, cmd.ack, self.parser)
        return self.response(req, consumer)
    
