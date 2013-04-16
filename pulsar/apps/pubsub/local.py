import re
import logging

from pulsar.apps import pubsub
from pulsar import send, command, get_actor
from pulsar.utils.pep import itervalues, iteritems


LOGGER = logging.getLogger('pulsar.pubsub')
        
        
class PubSub(pubsub.PubSub):
    '''Implements :class:`PubSub` in pulsar.
    
.. attribute:: monitor

    The name of :class:`pulsar.Monitor` (application) which broadcast messages
    to its workers.
'''
    def setup(self, **params):
        super(PubSub, self).setup(**params)
        self.actor = get_actor()
        pubsub = _get_pubsub(self.actor)
        if self.id not in pubsub:
            pubsub[self.id] = self
            
    def publish(self, channel, message):
        message = self.encode(message)
        return self.actor.send('arbiter', 'publish_message', self.id, channel,
                               message)
        
    def subscribe(self, channel, *channels):
        return self.actor.send('arbiter', 'pubsub_subscribe', self.id, channel,
                               *channels)
        
    def unsubscribe(self, *channels):
        return self.actor.send('arbiter', 'pubsub_unsubscribe', self.id,
                               *channels)
        
    def close(self):
        result = self.unsubscribe()
        self.actor.params.pubsub.pop(self.id, None)
        self.actor = None
        return result

    
@command()
def publish_message(request, id, channel, message):
    arbiter = request.actor
    clients = {}
    if not channel or channel == '*':
        matched = ((c, reg[1]) for c, reg in iteritems(_get_pubsub_channels(arbiter)))
    else:
        matched = _channel_groups(arbiter, channel)
    # loop over matched channels
    for channel, group in matched:
        for aid, pid in group:
            # if the id is matched we have a client
            if pid == id:
                if aid not in clients:
                    clients[aid] = []
                clients[aid].append(channel)
    for aid, channels in iteritems(clients):
        arbiter.send(aid, 'broadcast_message', id, channels, message)
    return len(clients)
        
@command()
def pubsub_subscribe(request, id, *channels):
    for channel in channels:
        _, group = _get_channel(request.actor, channel)
        group.add((request.caller.aid, id))
        
@command()
def pubsub_unsubscribe(request, id, *channels):
    client = (request.caller.aid, id)
    u = []
    pubsub_channels = _get_pubsub_channels(request.actor)
    channels = channels or list(pubsub_channels)
    for channel in channels:
        if channel in pubsub_channels:
            _, group =  pubsub_channels[channel]
            if client in group:
                u.append(channel)
                group.remove(client)
                if not group:
                    pubsub_channels.pop(channel)
    return u
    
@command(ack=False)
def broadcast_message(request, id, channels, message):
    '''In the actor domain'''
    pubsub = _get_pubsub(request.actor)
    handler = pubsub.get(id)
    if handler:
        handler.broadcast(channels, message)
    else:
        LOGGER.warning('Pubsub handler not available')
    
def _get_channel(arbiter, channel):
    pubsub = _get_pubsub_channels(arbiter)
    if channel not in pubsub:                         
        if '*' in channel:
            channel_re = re.compile(channel)
        else:
            channel_re = channel
        pubsub[channel] = (channel_re, set())
    return pubsub[channel]

def _get_pubsub_channels(actor):
    if 'pubsub_channels' not in actor.params:
        actor.params.pubsub_channels = {}
    return actor.params.pubsub_channels

def _get_pubsub(actor):
    if 'pubsub' not in actor.params:
        actor.params.pubsub = {}
    return actor.params.pubsub

def _channel_groups(actor, channel):
    for channel_re, group in itervalues(_get_pubsub_channels(actor)):
        if hasattr(channel_re, 'match'):
            g = channel_re.match(channel)
            if g:
                yield g.group(), group
        elif channel_re == channel:
            yield channel, group