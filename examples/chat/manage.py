'''A web-based chat application, it exposes three different
:ref:`wsgi handlers <wsgi-handlers>`:

* A :class:`pulsar.apps.wsgi.handlers.Router` to render the web page
* A :class:`pulsar.apps.ws.WebSocket` with the :class:`Chat` handler
* A :class:`pulsar.apps.wsgi.handlers.Router` with the :class:`Rpc` handler
  for exposing a :ref:`JSON-RPC <apps-rpc>` api.

To run the server::

    python manage.py
    
and open web browsers at http://localhost:8060

To send messages from the JSON RPC open a python shell and::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060/rpc', force_sync=True)
    >>> p.message('Hi from rpc')
    'OK'
    
Implementation
===========================

.. autoclass:: Chat
   :members:
   :member-order: bysource
   
.. autoclass:: Rpc
   :members:
   :member-order: bysource
   
.. autoclass:: WebChat
   :members:
   :member-order: bysource
'''
import os
import sys
import json
from random import random
import time
try:
    import pulsar
except ImportError: #pragma nocover
    sys.path.append('../../')
    import pulsar
from pulsar.utils.path import Path
from pulsar.apps import ws, wsgi, rpc, pubsub

CHAT_DIR = os.path.dirname(__file__)

stdnet = Path(__file__).add2python('stdnet', 3, down=['python-stdnet'],
                                   must_exist=False)
if stdnet:
    # Add option to use redis pubsub instead of local pubsub
    # If we are testsing don't do anything, the setting is already available
    import pulsar.utils.settings.backend


class PubSubClient(pubsub.Client):
    
    def __init__(self, connection):
        self.connection = connection
        
    def __call__(self, channel, message):
        if channel == 'webchat':
            self.connection.write(message)
        

##    Web Socket Chat handler
class Chat(ws.WS):
    '''The websocket handler (:class:`pulsar.apps.ws.WS`) managing the chat
application.

.. attribute:: pubsub

    The :ref:`publish/subscribe handler <apps-pubsub>` created by the wsgi
    application in the :meth:`WebChat.setup` method.
'''
    def __init__(self, pubsub):
        self.pubsub = pubsub
        
    def on_open(self, websocket):
        '''When a new websocket connection is established it creates a
:ref:`publish/subscribe <apps-pubsub>` client and adds it to the set
of clients of the :attr:`pubsub` handler.'''
        self.pubsub.add_client(PubSubClient(websocket))
        
    def on_message(self, websocket, msg):
        '''When a new message arrives, it publishes to all listening clients.'''
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                self.pubsub.publish('webchat', msg)


##    RPC MIDDLEWARE To publish messages
class Rpc(rpc.PulsarServerCommands):
    
    def __init__(self, pubsub, **kwargs):
        self.pubsub = pubsub
        super(Rpc, self).__init__(**kwargs)
        
    def rpc_message(self, request, message):
        '''Publish a message via JSON-RPC'''
        self.pubsub.publish('webchat', message)
        return 'OK'
    
    
class WebChat(wsgi.LazyWsgi):
    '''This is the :ref:`wsgi application <wsgi-handlers>` for this
web-chat example.'''
    def __init__(self, server_name):
        self.name = server_name
        
    def setup(self):
        '''This method is called once only to setup the WSGI application
handler as described in :ref:`lazy wsgi handler <wsgi-lazy-handler>`
section. It creates a :ref:`publish/subscribe handler <apps-pubsub>`
and subscribe it to the ``webchat`` channel.'''
        backend = self.cfg.get('backend_server')
        self.pubsub = pubsub.PubSub(backend, encoder=self.encode_message)
        self.pubsub.subscribe('webchat')
        return wsgi.WsgiHandler([wsgi.Router('/', get=self.home_page),
                                 ws.WebSocket('/message', Chat(self.pubsub)),
                                 wsgi.Router('/rpc', post=Rpc(self.pubsub))])
        
    def home_page(self, request):
        data = open(os.path.join(CHAT_DIR, 'chat.html')).read()
        request.response.content_type = 'text/html'
        request.response.content = data % request.environ
        return request.response
    
    def encode_message(self, message):
        if not isinstance(message, dict):
            message = {'message': message}
        message['time'] = time.time()
        return json.dumps(message)

    @property
    def cfg(self):
        '''Get the ``config`` object from the actor serving the webchat.'''
        actor = pulsar.get_actor()
        if actor.is_arbiter():
            actor = actor.get_actor(self.name)
        return actor.cfg


def server(callable=None, name=None, **kwargs):
    name = name or 'wsgi'
    return wsgi.WSGIServer(callable=WebChat(name), name=name, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()
