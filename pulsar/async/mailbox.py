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
                             new_event_loop
from pulsar.utils.sockets import nice_address
from pulsar.utils.websocket import FrameParser
from pulsar.utils.security import gen_unique_id

from .access import get_actor, set_actor, PulsarThread
from .defer import make_async, log_failure, Deferred
from .transports import ProtocolConsumer, Client, Request
from .proxy import actorid, get_proxy, get_command, CommandError, ActorProxy


LOGGER = logging.getLogger('pulsar.mailbox')
    
CommandRequest = namedtuple('CommandRequest', 'actor caller connection')
    
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
    

class Message(Request):
    
    def __init__(self, command, sender, target, args, kwargs, address=None,
                 timeout=None):
        super(Message, self).__init__(address, timeout)
        # Build the request and write
        command = get_command(command)
        self.data = {'command': command.__name__,
                     'sender': actorid(sender),
                     'target': actorid(target),
                     'args': args if args is not None else (),
                     'kwargs': kwargs if kwargs is not None else {}}
        if command.ack:
            self.consumer = Deferred()
            self.data['ack'] = gen_unique_id()[:8]
        else:
            self.consumer = None


class MailboxConsumer(ProtocolConsumer):

    def __init__(self, *args, **kwargs):
        super(MailboxConsumer, self).__init__(*args, **kwargs)
        self._pending_responses = {}
        self._parser = FrameParser(kind=2)
    
    def data_received(self, data):
        # Feed data into the parser
        #LOGGER.debug('Got new data of %s length', len(data))
        msg = self._parser.decode(data)
        while msg:
            try:
                message = pickle.loads(msg.body)
            except Exception:
                raise ProtocolError('Could not decode message body')
            log_failure(self.responde(message))
            msg = self._parser.decode()
    
    def start_request(self):
        # Always called in thread
        req = self.request
        if req.consumer and 'ack' in req.data:
            self._pending_responses[req.data['ack']] = req.consumer
        obj = pickle.dumps(req.data, protocol=2)
        data = self._parser.encode(obj, opcode=0x2).msg
        self.transport.write(data)
        
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
            #LOGGER.debug('handling message %s', command)
            if command == 'callback':   #this is a callback
                return self.callback(message.get('ack'), message.get('result'))
            target = actor.get_actor(message['target'])
            if target is None:
                raise CommandError('unknown actor %s' % message['target'])
            if isinstance(target, ActorProxy):
                # route the message to the actor
                raise NotImplementedError()
            else:
                actor = target
            caller = actor.get_actor(message['sender'])
            command = get_command(command)
            req = CommandRequest(target, get_proxy(caller, safe=True),
                                 self.connection)
            result = command(req, message['args'], message['kwargs'])
        except Exception:
            result = sys.exc_info()
        return make_async(result).add_both(partial(self._responde, message))
        
    def _responde(self, data, result):
        if data.get('ack'):
            self._request = {'command': 'callback', 'result': result,
                             'ack': data['ack']}
            self.start_request()
        #Return the result so a failure can be logged
        return result

    def request(self, command, sender, target, args, kwargs):
        '''Used by the server'''
        req = Message(command, sender, target, args, kwargs)
        self.new_request(req)
        return req.consumer

    
class MailboxClient(Client):
    # mailbox for actors client
    consumer_factory = MailboxConsumer
    max_connections = 1
     
    def __init__(self, address, actor):
        super(MailboxClient, self).__init__()
        self.address = address
        self.consumer = None
        self.name = 'Mailbox for %s' % actor
        eventloop = actor.requestloop
        # The eventloop is cpubound
        if actor.cpubound:
            eventloop = new_event_loop()
            set_event_loop(eventloop)
            # starts in a new thread
            actor.requestloop.call_soon_threadsafe(self._start_on_thread)
        # when the mailbox shutdown, the event loop must stop.
        self.bind_event('finish', lambda s: s.event_loop.stop())
        self._event_loop = eventloop
    
    def __repr__(self):
        return '%s %s' % (self.__class__.__name__, nice_address(self.address))
    
    @property
    def event_loop(self):
        return self._event_loop
    
    def request(self, command, sender, target, args, kwargs):
        red = Message(command, sender, target, args, kwargs,
                      self.address, self.timeout)
        self._event_loop.call_soon_threadsafe(self._request, req)
        return req.consumer
    
    def _request(self, req):
        if not self.consumer:
            self.consumer = self.response(req)
        else:
            self.consumer.new_request(req)
        
    def _start_on_thread(self):
        PulsarThread(name=self.name, target=self._event_loop.run).start()
        