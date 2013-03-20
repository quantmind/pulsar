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


When using this application, one starts by creating a :class:`PubSub` handler
in the actor process domain::

    from pulsar.apps import pubsub
    
    pubsub.register_handler(pubsub.PulsarPubSub(monitor_name))
    

The :class:`PulsarPubSub` is an implementation of the :class:`PubSub` interface
which uses a :class:`pulsar.Monitor` as the broadcast server.
    
    
API functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: register_handler


.. autofunction:: add_client


.. autofunction:: publish


PubSub Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource 
   
   
Pulsar PubSub
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PulsarPubSub
   :members:
   :member-order: bysource 
   
   
.. _wikipedia: http://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern'''
import time
import json

from pulsar import send, command, get_actor
from pulsar.utils.pep import to_string
from pulsar.utils.log import LocalMixin, local_property

################################################################################
##    API methods
def register_handler(handler, name=None):
    '''Register the current :class:`pulsar.Actor` with a :class:`PubSub`
*handler*.

:param handler: a :class:`PubSub` instance.
:param name: Optional string used as the key which holds the handler
    in the current :attr:`pulsar.Actor.params` attribute dictionary.
    Buy default it uses ``'pubsub'``. You can use this parameter to create
    several :class:`PubSub` handlers for a given actor.
'''
    actor = get_actor()
    actor.params[name or 'pubsub'] = handler 
    
def add_client(client, name=None):
    '''Add a new *client* to the current actor :class:`PubSub` handler
at *name*.

:param client: a client is any instance with a ``write`` method.
:param name: Optional string for the :class:`PubSub` to add the client to.
    If not provided the default ``'pubsub'`` key is used.
'''
    get_pubsub(name).add_client(client)
    
def publish(message, name=None):
    '''Publish a *message* to the :class:`PubSub` handler at *name*.'''
    get_pubsub(name).publish(message)
    
def get_pubsub(name=None):
    pubsub = get_actor().params.get(name or 'pubsub')
    if pubsub is None:
        raise RuntimeError('Publish/Subscribe handler not available')
    return pubsub
    
################################################################################
##    PubSub Interface
class PubSub(LocalMixin):
    '''Interface for Publish/Subscribe paradigm.
    
.. attribute:: clients

    Set of all clients for this :class:`PubSub` handler.
'''
    @local_property
    def clients(self):
        return set()
    
    def add_client(self, client):
        '''Add a new *client* to the set of all :attr:`clients`.'''
        self.clients.add(client)
        
    def publish(self, message):
        '''Publish a *message*. Must be implemented by subclasses.'''
        raise NotImplementedError
            
    def encode(self, message):
        '''Encode *message* before publishing it. By default it create a
dictionary with a timestamp and the message and serialise it as json.'''
        message = {'time': time.time(), 'message': message}
        return json.dumps(message)
    
    def decode(self, message):
        '''Convert message to a string. The behaviour can be overwritten.'''
        return to_string(message)
    
    def broadcast(self, message):
        remove = set()
        message = self.decode(message)
        for client in self.clients:
            try:
                client.write(message)
            except Exception:
                remove.add(client)
        self.clients.difference_update(remove)

################################################################################
##    Pulsar PubSub implementation        
class PulsarPubSub(PubSub):
    '''Implements :class:`PubSub` in pulsar.
    
.. attribute:: monitor

    The name of :class:`pulsar.Monitor` (application) which broadcast messages
    to its workers.
'''
    def __init__(self, monitor):
        self._monitor = monitor
            
    def publish(self, message):
        '''Implements :meth:`PubSub.publish`. It calls the :meth:`encode`
method to encode the message and than send the encoded message to the
:class:`pulsar.Monitor` specified in the :attr:`monitor` attribute.'''
        message = self.encode(message)
        send(self._monitor, 'publish_message', message)
        
    
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
