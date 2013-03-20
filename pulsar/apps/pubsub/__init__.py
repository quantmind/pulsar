'''A framework for a Publish/Subscribe pattern.

.. epigraph::

    In software architecture, publish/subscribe is a messaging pattern where
    senders of messages, called publishers, do not program the messages to
    be sent directly to specific receivers, called subscribers.
    Instead, published messages are characterized into classes, without
    knowledge of what, if any, subscribers there may be. Similarly, subscribers
    express interest in one or more classes, and only receive messages that are
    of interest, without knowledge of what, if any, publishers there are.
    
    -- wikipedia_


.. autofunction:: register_handler


.. autofunction:: subscribe


.. autofunction:: publish


PubSub Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource 
   
   
.. _wikipedia: http://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern'''
import time
import json
from pulsar import send, command, get_actor

################################################################################
##    API methods
def register_handler(handler, name=None):
    '''Register the current :class:`pulsar.Actor` with a :class:`PubSub`
*handler*.'''
    actor = get_actor()
    actor.params[name or 'pubsub'] = handler 
    
def subscribe(client, **params):
    '''Subscribe a new *client* to the current actor :class:`PubSub` handler.'''
    get_pubsub().subscribe(client, **params)
    
def publish(message):
    '''Publish a message'''
    get_pubsub().publish(message)
    
def get_pubsub():
    pubsub = get_actor().params.pubsub
    if pubsub is None:
        raise RuntimeError('Publish/Subscribe handler not available')
    return pubsub
    
################################################################################
##    PubSub Interface
class PubSub(object):
    '''Interface for Publish/Subscribe paradigm'''
    
    def subscribe(self, client):
        '''Subscribe a *client* with this :class:`PubSub` handler.
Must be implemented by subclasses.'''
        raise NotImplementedError
    
    def publish(self, message):
        '''Publish a *message*. Must be implemented by subclasses.'''
        raise NotImplementedError
            

################################################################################
##    Pulsar PubSub implementation        
class PulsarPubSub(PubSub):
    '''Implements :class:`PubSub` in pulsar.
    
.. attribute:: monitor

    The name of :class:`pulsar.Monitor` (application) which broad cast messages
    to its workers.
'''
    def __init__(self, monitor):
        self._clients = set()
        self._monitor = monitor
    
    def subscribe(self, client):
        self._clients.add(client)
            
    def publish(self, message):
        '''Implements :meth:`PubSub.publish`. It calls the :meth:`encode`
method to encode the message and than send the encoded message to the
:class:`pulsar.Monitor` specified in the :attr:`monitor` attribute.'''
        message = self.encode(message)
        send(self._monitor, 'publish_message', message)
            
    def encode(self, message):
        '''Encode *message* before publishing it.'''
        message = {'time': time.time(), 'message': message}
        return json.dumps(message)
    
    def broadcast(self, message):
        remove = set()
        for client in self._clients:
            try:
                client.write(message)
            except Exception:
                remove.add(client)
        self._clients.difference_update(remove)
        
    
@command(ack=False)
def publish_message(request, message):
    monitor = request.actor
    if monitor.managed_actors:
        for worker in monitor.managed_actors.values():
            monitor.send(worker, 'broadcast_message', message)
    else:
        get_pubsub().broadcast(message)
        
@command(ack=False)
def broadcast_message(request, message):
    '''In the actor domain'''
    get_pubsub().broadcast(message)

################################################################################