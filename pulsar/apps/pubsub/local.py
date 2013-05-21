import re
import logging

from pulsar.apps import pubsub
from pulsar import send, command
from pulsar.utils.pep import itervalues, iteritems


LOGGER = logging.getLogger('pulsar.pubsub')
        
        
class PubSubBackend(pubsub.PubSubBackend):
    '''Implements :class:`PubSub` in pulsar.
    
.. attribute:: monitor

    The name of :class:`pulsar.Monitor` (application) which broadcast messages
    to its workers.
'''
    def publish(self, channel, message):
        return send(self.name, 'pubsub_publish', self.id, channel, message)
        
    def subscribe(self, *channels):
        return send(self.name, 'pubsub_subscribe', self.id, *channels)
        
    def unsubscribe(self, *channels):
        return send(self.name, 'pubsub_unsubscribe', self.id, *channels)


@command()
def pubsub_publish(request, id, channel, message):
    monitor = request.actor
    if not channel or channel == '*':
        matched = ((c, reg[1]) for c, reg in iteritems(_get_pubsub_channels(monitor)))
    else:
        matched = _channel_groups(monitor, channel)
    clients = set()
    # loop over matched channels
    for channel, group in matched:
        for aid, pid in group:
            # if the id is matched we have a client
            if pid == id:
                clients.add(aid)
                monitor.send(aid, 'pubsub_broadcast', id, channel, message)
    return len(clients)
        
@command()
def pubsub_subscribe(request, id, *channels):
    client = (request.caller.aid, id)
    for channel in channels:
        _, group = _get_channel(request.actor, channel)
        group.add(client)
    return _channels_for_client(request.actor, client)
        
@command()
def pubsub_unsubscribe(request, id, *channels):
    client = (request.caller.aid, id)
    pubsub_channels = _get_pubsub_channels(request.actor)
    channels = channels or list(pubsub_channels)
    for channel in channels:
        if channel in pubsub_channels:
            _, group =  pubsub_channels[channel]
            group.discard(client)
            if not group:
                pubsub_channels.pop(channel)
    return _channels_for_client(request.actor, client)
    
@command(ack=False)
def pubsub_broadcast(request, id, channels, message):
    '''In the actor domain'''
    pubsub = PubSubBackend.get(id, actor=request.actor)
    if pubsub:
        pubsub.broadcast(channels, message)
    else:
        LOGGER.warning('Pubsub backend not available in %s', request.actor)
    
def _get_channel(monitor, channel):
    pubsub = _get_pubsub_channels(monitor)
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

def _channel_groups(actor, channel):
    for channel_re, group in itervalues(_get_pubsub_channels(actor)):
        if hasattr(channel_re, 'match'):
            g = channel_re.match(channel)
            if g:
                yield g.group(), group
        elif channel_re == channel:
            yield channel, group
            
def _channels_for_client(monitor, client):
    c = 0
    for _, group in _get_pubsub_channels(monitor).values():
        if client in group:
            c += 1
    return c