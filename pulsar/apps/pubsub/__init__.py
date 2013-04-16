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


When using this application, one starts by creating a :class:`PubSub` handler::

    from pulsar.apps.pubsub import PubSub
    
    pubsub = PubSub.make(connection_string=None, ...)
    
The ``connection_string`` parameter is needed in order to select the backend
to used. If not supplied, the default ``local://`` backend is used.


PubSub Backend Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource 
   
   
.. _wikipedia: http://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern'''
import time
import json

import pulsar
from pulsar.utils.pep import to_string
from pulsar.utils.log import local_property
    
################################################################################
##    PubSub Interface
class PubSub(pulsar.Backend):
    '''Interface for Publish/Subscribe paradigm Backend.
    
.. attribute:: channel

    The Channel name for this :class:`PubSub` handler. If not supplied the
    :attr:`pulsar.apps.Backend.name` attribute is used.
    
.. attribute:: clients

    Set of all clients for this :class:`PubSub` handler.
'''
    default_path = 'pulsar.apps.pubsub.%s'
        
    @local_property
    def clients(self):
        '''The set of clients for this :class:`PubSub` handler.'''
        return set()
    
    def close(self):
        '''Close connections'''
        pass
    
    def add_client(self, client):
        '''Add a new *client* to the set of all :attr:`clients`. Clients
must have the ``write`` method available. When a new message is received
from the publisher, the :meth:`broadcast` method will notify all
:attr:`clients` via the ``write`` method.'''
        self.clients.add(client)
        
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
            
    def encode(self, message):
        '''Encode *message* before publishing it. By default it create a
dictionary with a timestamp and the message and serialise it as json.'''
        if not isinstance(message, dict):
            message = {'message': message}
        message['time'] = time.time()
        return json.dumps(message)
    
    def decode(self, message):
        '''Convert message to a string. The behaviour can be overwritten.'''
        return to_string(message)
    
    def broadcast(self, channels, message):
        '''Broadcast ``message`` to all :attr:`clients`.'''
        remove = set()
        message = self.decode(message)
        for client in self.clients:
            try:
                client.write(message)
            except Exception:
                remove.add(client)
        self.clients.difference_update(remove)
