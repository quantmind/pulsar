'''Pulsar internal communication uses the websocket protocol.'''
import sys
import logging
import tempfile
from functools import partial

from pulsar import platform, PulsarException, Config
from pulsar.utils.pep import to_bytes, ispy3k, ispy3k, pickle, set_event_loop
from pulsar.utils.websocket import FrameParser

from .access import get_actor, set_actor
from .defer import make_async, log_failure, is_failure
from .servers import create_server, ServerConnection
from .protocols import ProtocolConsumer
from .proxy import actorid, CommandNotFound
from . import clients


__all__ = ['mailbox', 'CommandNotFound']


LOGGER = logging.getLogger('pulsar.mailbox')


def mailbox(actor=None, address=None):
    '''Creates a :class:`Mailbox` for *actor*.'''
    if address:
        #if platform.type == 'posix':
        #    address = 'unix:%s' % address
        return PulsarClient(address)
    elif actor.is_monitor():
        return MonitorMailbox(actor)
    else:
        #if platform.type == 'posix':
        #    address = 'unix:%s.pulsar' % actor.aid
        #else:   #pragma    nocover
        #    address = ('127.0.0.1', 0)
        address = ('127.0.0.1', 0)
        server = actor.requestloop.create_server(address=address,
                                        name='Mailbox for %s' % actor,
                                        connection_factory=MailboxConnection,
                                        response_factory=MailboxResponse,
                                        timeout=3600,
                                        close_event_loop=True)
        server.event_loop.call_soon(send_mailbox_address, actor)
        return server
    
    
def dump_data(obj):
    return pickle.dumps(obj, protocol=2)

def load_message(data):
    return pickle.loads(data)
    
    
class Request(clients.Request):
    # Actor message request
    def __init__(self, address, timeout, command, sender, receiver,
                 args, kwargs, ack):
        super(Request, self).__init__(address, timeout)
        self.data = {'command': command,
                     'sender': actorid(sender),
                     'receiver': actorid(receiver),
                     'args': args if args is not None else (),
                     'kwargs': kwargs if kwargs is not None else {}}
        self.parser = FrameParser(kind=1)
        self.ack = ack
    
    def encode(self): 
        msg = dump_data(self.data)
        return self.parser.encode(msg).msg

    def __repr__(self):
        return self.data['command']
    __str__ = __repr__


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
        self.parser = FrameParser()
        
    def feed(self, data):
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        msg = self.parser.execute(data)
        if msg:
            message = load_message(msg.body)
            self.responde(message)
    
    def encode(self, code, data=None): 
        msg = dump_data((code, data))
        frame = self.parser.encode(msg)
        return frame.msg
    
    def responde(self, message):
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
                msg = self.encode('errback')
                self.transport.write(msg)
        elif ack:
            msg = self.encode('callback', result)
            self.transport.write(msg)
    

################################################################################
##    Mailbox Client Classes
class MailboxClientConsumer(clients.ClientProtocolConsumer):
    '''The Protocol consumer for a Mailbox client'''
    def feed(self, data):
        frame = self.request.parser.execute(data)
        if frame:
            code, result = load_message(frame.body)
            try:
                if code == 'errback':
                    raise PulsarException
            except:
                result = sys.exc_info()
            self.result = result
            self.finished(result)
    
    def send(self, *args):
        message = self.request.encode()
        self.write(message)
        if not self.request.ack: # No acknowledgment
            self.finished()
    
    
class PulsarClient(clients.Client):
    response_factory = MailboxClientConsumer
     
    def __init__(self, address, **params):
        super(PulsarClient, self).__init__(**params)
        self.address = address
        
    def request(self, cmd, sender, target, args, kwargs, consumer=None):
        req = Request(self.address, self.timeout, cmd.__name__, sender,
                      target, args, kwargs, cmd.ack)
        return self.response(req, consumer)
    
    
def send_mailbox_address(actor):
    actor.logger.info('%s started at address %s', actor, actor.mailbox)
    a = get_actor()
    if a is not actor:
        set_actor(actor)
    actor.register()
