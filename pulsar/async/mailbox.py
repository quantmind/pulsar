'''Actors communicate with each other by sending and receiving messages.
The :mod:`pulsar.async.mailbox` module implements the message passing layer
via a bidirectional socket connections between the :class:`Arbiter`
and actors.'''
import sys
import logging
import tempfile
from functools import partial
from collections import namedtuple

from pulsar import platform, PulsarException, Config, ProtocolError
from pulsar.utils.pep import to_bytes, ispy3k, ispy3k, pickle, set_event_loop,\
                             get_event_loop, new_event_loop
from pulsar.utils.websocket import FrameParser
from pulsar.utils.security import gen_unique_id

from .access import get_actor, set_actor, PulsarThread
from .defer import make_async, log_failure, Deferred
from .servers import create_server, ServerConnection
from .protocols import ProtocolConsumer
from .proxy import actorid, get_proxy, get_command
from . import clients


LOGGER = logging.getLogger('pulsar.mailbox')
    
Request = namedtuple('Request', 'actor caller connection')
    
class MonitorMailbox(object):
    '''A :class:`Mailbox` for a :class:`Monitor`. This is a proxy for the
arbiter mailbox.'''
    active_connections = 0
    def __init__(self, actor):
        self.mailbox = actor.monitor.mailbox
        # make sure the monitor get the hand shake!
        self.mailbox.event_loop.call_soon_threadsafe(actor.hand_shake)

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
    
        
class MailboxMixin(object):
    # A Mixin for both Server and Client protocol consumers
    def __new__(cls, *args, **kwargs):
        self = super(MailboxMixin, cls).__new__(cls)
        self._pending_responses = {}
        self._parser = FrameParser(kind=2)
        return self
    
    def feed(self, data):
        # The receiver could be different from the mail box actor. For
        # example a monitor uses the same mailbox as the arbiter
        msg = self._parser.decode(data)
        if msg:
            message = pickle.loads(msg.body)
            log_failure(self.responde(message))
    
    def create_request(self, command, sender, target, args, kwargs):
        # Build the request and write
        command = get_command(command)
        data = {'command': command.__name__,
                'sender': actorid(sender),
                'receiver': actorid(target),
                'args': args if args is not None else (),
                'kwargs': kwargs if kwargs is not None else {}}
        d = None
        if command.ack:
            d = Deferred()
            data['ack'] = gen_unique_id()[:8]
        return data, d
    
    def callback(self, ack, result):
        if not ack:
            raise ProtocolError('A callback without id')
        try:
            pending = self._pending_responses.pop(ack)
        except KeyError:
            raise KeyError('Callback %s not in pending callbacks' % ack)
        pending.callback(result)
        
    def responde(self, message):
        actor = get_actor()
        try:
            command = message['command']
            if command == 'callback':   #this is a callback
                return self.callback(message.get('ack'), message.get('result'))
            actor = actor.get_actor(message['receiver']) or actor
            caller = actor.get_actor(message['sender'])
            command = get_command(command)
            req = Request(actor, get_proxy(caller, safe=True), self.connection)
            result = command(req, message['args'], message['kwargs'])
        except:
            result = sys.exc_info()
        return make_async(result).add_both(partial(self._responde, message))
        
    def _responde(self, data, result):
        if data.get('ack'):
            data = {'command': 'callback', 'result': result, 'ack': data['ack']}
            self.write(data)
        #Return the result so a failure can be logged
        return result
            
    def dump_data(self, obj):
        obj = pickle.dumps(obj, protocol=2)
        return self._parser.encode(obj).msg

    def write(self, data, consumer=None):
        if consumer and 'ack' in data:
            self._pending_responses[data['ack']] = consumer
        data = self.dump_data(data)
        self._write(data)
    
    def _write(self, data):
        self.transport.write(data)

################################################################################
##    Mailbox Server (Arbiter) Classes
class MailboxConnection(ServerConnection):
    authenticated = False
    

class MailboxServerConsumer(MailboxMixin, ProtocolConsumer):
 
    def request(self, command, sender, target, args, kwargs):
        data, d = self.create_request(command, sender, target, args, kwargs)
        self.write(data, d)
        return d
        
    
################################################################################
##    Mailbox Client (Actor) Classes
class MailboxClientConsumer(MailboxMixin, clients.ClientProtocolConsumer):
    '''The Protocol consumer for a Mailbox client'''    
    def send(self, *args):
        # This is only called at the first request
        pass
    
    def _write(self, data):
        self.event_loop.call_soon_threadsafe(self.transport.write, data)
    
    
class MailboxClient(clients.Client):
    # mailbox for actors client
    consumer_factory = MailboxClientConsumer
     
    def __init__(self, address, actor):
        super(MailboxClient, self).__init__(max_connections=1)
        self.address = address
        self.consumer = None
        self.name = 'Mailbox for %s' % actor
        self._cpubound = False
        eventloop = get_event_loop()
        # The eventloop is cpubound
        if eventloop is None or getattr(eventloop, 'cpubound', False):
            # No IO event loop available in the current thread.
            # Create one and set it as the event loop
            self._cpubound = True
            eventloop = new_event_loop()
            set_event_loop(eventloop)
            actor.requestloop.call_soon_threadsafe(self._start_on_thread)
        self._event_loop = eventloop
    
    @property
    def event_loop(self):
        return self._event_loop
    
    def request(self, command, sender, target, args, kwargs):
        # Build the request and write
        if not self.consumer:
            req = clients.Request(self.address, self.timeout)
            self.consumer = self.response(req)
        c = self.consumer
        data, d = c.create_request(command, sender, target, args, kwargs)
        if c.protocol.on_connection.called:
            c.write(data, d)
        else:
            c.protocol.on_connection.add_callback(lambda r: c.write(data, d))
        return d
        
    
    def _start_on_thread(self):
        PulsarThread(name=self.name, target=self._event_loop.run).start()
        
    def connect(self):
        request = clients.Request(self.address, self.timeout)
        self.consumer = self.response(request)
    
    def close(self):
        if self._cpubound:
            self._event_loop.stop()
        return super(MailboxClient, self).close()

