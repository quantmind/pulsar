'''Actors communicate with each other by sending and receiving messages.
The :mod:`pulsar.async.mailbox` module implements the message passing layer
via a bidirectional socket connections between the :class:`pulsar.Arbiter`
and :class:`pulsar.Actor`.'''
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
from .defer import async, maybe_failure, log_failure, Deferred
from .transports import ProtocolConsumer, SingleClient, Request
from .proxy import actorid, get_proxy, get_command, CommandError, ActorProxy


LOGGER = logging.getLogger('pulsar.mailbox')
    
CommandRequest = namedtuple('CommandRequest', 'actor caller connection')

def command_in_context(command, caller, actor, args, kwargs):
    cmnd = get_command(command)
    if not cmnd:
        raise CommandError('unknown %s' % command)
    request = CommandRequest(actor, caller, None)
    return cmnd(request, args, kwargs)
    
    
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
    '''A message which travels from actor to actor.'''
    def __init__(self, data, future=None, address=None, timeout=None):
        super(Message, self).__init__(address, timeout)
        self.data = data
        self.future = future
    
    @classmethod
    def command(cls, command, sender, target, args, kwargs, address=None,
                 timeout=None):
        command = get_command(command)
        data = {'command': command.__name__,
                'sender': actorid(sender),
                'target': actorid(target),
                'args': args if args is not None else (),
                'kwargs': kwargs if kwargs is not None else {}}
        if command.ack:
            future = Deferred()
            data['ack'] = gen_unique_id()[:8]
        else:
            future = None
        return cls(data, future, address, timeout)
        
    @classmethod
    def callback(cls, result, ack):
        data = {'command': 'callback', 'result': result, 'ack': ack}
        return cls(data)
        

class MailboxConsumer(ProtocolConsumer):

    def __init__(self, *args, **kwargs):
        super(MailboxConsumer, self).__init__(*args, **kwargs)
        self._pending_responses = {}
        self._parser = FrameParser(kind=2)
    
    def request(self, command, sender, target, args, kwargs):
        '''Used by the server to send messages to the client.'''
        req = Message.command(command, sender, target, args, kwargs)
        self.new_request(req)
        return req.future
    
    ############################################################################
    ##    PROTOCOL CONSUMER IMPLEMENTATION
    def data_received(self, data):
        # Feed data into the parser
        msg = self._parser.decode(data)
        while msg:
            try:
                message = pickle.loads(msg.body)
            except Exception:
                raise ProtocolError('Could not decode message body')
            log_failure(self._responde(message))
            msg = self._parser.decode()
    
    def start_request(self):
        # Always called in thread
        req = self.current_request
        if req.future and 'ack' in req.data:
            self._pending_responses[req.data['ack']] = req.future
            try:
                self._write(req)
            except Exception as e:
                req.future.callback(e)
        else:
            self._write(req)
    
    ############################################################################
    ##    INTERNALS
    @async(max_errors=0)
    def _responde(self, message):
        actor = get_actor()
        command = message.get('command')
        LOGGER.debug('%s handling message "%s"', actor, command)
        if command == 'callback':   #this is a callback
            self._callback(message.get('ack'), message.get('result'))
        else:
            try:
                target = actor.get_actor(message['target'])
                if target is None:
                    raise CommandError('unknown actor %s' % message['target'])
                caller = get_proxy(actor.get_actor(message['sender']), safe=True)
                if isinstance(target, ActorProxy):
                    # route the message to the actor proxy
                    if caller is None:
                        raise CommandError("'%s' got message from unknown '%s'" %
                                           (actor, message['sender']))
                    result = yield actor.send(target, command, *message['args'],
                                              **message['kwargs'])
                else:
                    actor = target
                    command = get_command(command)
                    req = CommandRequest(target, caller, self.connection)
                    result = yield command(req, message['args'], message['kwargs'])
            except Exception as e:
                result = maybe_failure(e)
            yield self._response(message, result)
        
    def _response(self, data, result):
        if data.get('ack'):
            req = Message.callback(result, data['ack'])
            self.new_request(req)
        #Return the result so a failure can be logged
        return result

    def _callback(self, ack, result):
        if not ack:
            raise ProtocolError('A callback without id')
        try:
            pending = self._pending_responses.pop(ack)
        except KeyError:
            raise KeyError('Callback %s not in pending callbacks' % ack)
        pending.callback(result)
        
    def _write(self, req):
        obj = pickle.dumps(req.data, protocol=2)
        data = self._parser.encode(obj, opcode=0x2).msg
        self.transport.write(data)
        
    
class MailboxClient(SingleClient):
    # mailbox for actors client
    consumer_factory = MailboxConsumer
    max_reconnect = 0
     
    def __init__(self, address, actor):
        super(MailboxClient, self).__init__(address)
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
        # the request method
        req = Message.command(command, sender, target, args, kwargs,
                              self.address, self.timeout)
        # we make sure responses are run on the event loop thread
        self._event_loop.call_now_threadsafe(self.response, req)
        return req.future
        
    def _start_on_thread(self):
        PulsarThread(name=self.name, target=self._event_loop.run).start()
        