'''The :mod:`pulsar.apps.pubsub` implements a middleware
handler for the Publish/Subscribe pattern.

.. epigraph::

    In software architecture, publish/subscribe is a messaging pattern where
    senders of messages, called publishers, do not program the messages to
    be sent directly to specific receivers, called subscribers.
    Instead, published messages are characterized into classes, without
    knowledge of what, if any, subscribers there may be. Similarly, subscribers
    express interest in one or more classes, and only receive messages that are
    of interest, without knowledge of what, if any, publishers there are.
    
    -- wikipedia_


When using this middleware, one starts by creating a :class:`PubSub` handler::

    from pulsar.apps.pubsub import PubSub
    
    pubsub = PubSub(connection_string=None, ...)
    
The ``connection_string`` parameter is needed in order to select the backend
to use. If not supplied, the default ``local://`` backend is used.


PubSub handler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource
   
   
PubSub backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PubSubBackend
   :members:
   :member-order: bysource 
   
   
.. _wikipedia: http://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern'''
import time
import json

import pulsar
from pulsar import get_actor
from pulsar.utils.pep import to_string
from pulsar.utils.log import local_property
    
################################################################################
##    PubSub Interface
class PubSub(object):
    '''Interface for Publish/Subscribe paradigm Backend.

.. attribute:: backend

    The :class:`PubSubBackend` for this handler.
        
.. attribute:: clients

    Set of all clients for this :class:`PubSub` handler.
'''
    def __init__(self, backend=None, **params):
        be = PubSubBackend.make(backend=backend, **params)
        self.backend = PubSubBackend.get(be.id, backend=be)
    
    @property
    def clients(self):
        return self.backend.clients
    
    def add_client(self, client):
        '''Add a new *client* to the set of all :attr:`clients`. Clients
must have the ``write`` method available. When a new message is received
from the publisher, the :meth:`broadcast` method will notify all
:attr:`clients` via the ``write`` method.'''
        self.backend.add_client(client)
        
    def remove_client(self, client):
        '''Remove *client* from the set of all :attr:`clients`.'''
        self.backend.remove_client(client)
        
    def publish(self, channel, message):
        '''Publish a *message*. Must be implemented by subclasses.'''
        message = self.encode(message)
        return self.backend.publish(channel, message)
    
    def subscribe(self, *channels):
        '''Subscribe to the server which publish messages. Must be
implemented by subclasses.'''
        return self.backend.subscribe(*channels)
    
    def unsubscribe(self, *channels):
        '''Un-subscribe to the server which publish messages. Must be
implemented by subclasses.'''
        return self.backend.unsubscribe(*channels)
    
    def close(self):
        '''Close connections'''
        return self.backend.close()
    
    def encode(self, message):
        '''Encode *message* before publishing it. By default it create a
dictionary with a timestamp and the message and serialise it as json.'''
        if not isinstance(message, dict):
            message = {'message': message}
        message['time'] = time.time()
        return json.dumps(message)
    
    def __setstate__(self, state):
        super(PubSub, self).__setstate(state)
        self.backend = _pubsub_backend(self.backend.id, backend=self.backend)


class PubSubBackend(pulsar.Backend):
    '''Publish/Subscribe Backend interface.
    
.. attribute:: clients

    Set of all clients for this :class:`PubSub` handler.
'''
    default_path = 'pulsar.apps.pubsub.%s'
    
    @local_property
    def clients(self):
        '''The set of clients for this :class:`PubSub` handler.'''
        return set()
    
    def add_client(self, client):
        '''Add a new *client* to the set of all :attr:`clients`. Clients
must have the ``write`` method available. When a new message is received
from the publisher, the :meth:`broadcast` method will notify all
:attr:`clients` via the ``write`` method.'''
        self.clients.add(client)
        
    def remove_client(self, client):
        '''Remove *client* from the set of all :attr:`clients`.'''
        self.clients.discard(client)
        
    def publish(self, channel, message):
        '''Publish a *message*. Must be implemented by subclasses.'''
        raise NotImplementedError
    
    def subscribe(self, *channels):
        '''Subscribe to the server which publish messages. Must be
implemented by subclasses.'''
        raise NotImplementedError
    
    def unsubscribe(self, *channels):
        '''Un-subscribe to the server which publish messages. Must be
implemented by subclasses.'''
        raise NotImplementedError
        
    def broadcast(self, channels, message):
        '''Broadcast ``message`` to all :attr:`clients`.'''
        remove = set()
        message = self.decode(message)
        for client in self.clients:
            try:
                client.write(message)
            except Exception:
                remove.add(client)
    
    def close(self):
        '''Close this :class:`PubSubBackend`.'''
        actor = get_actor()
        actor.params.pubsub.pop(self.id, None)
        return self.unsubscribe()
    
    def decode(self, message):
        '''Convert message to a string.'''
        return to_string(message)
    
    @classmethod
    def get(cls, id, actor=None, backend=None):
        actor = actor or get_actor()
        if 'pubsub' not in actor.params:
            actor.params.pubsub = {}
        be = actor.params.pubsub.get(id)
        if not be and backend:
            be = backend
            actor.params.pubsub[id] = be
        return be