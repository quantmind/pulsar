'''This example is web-based chat application which exposes three different
:ref:`wsgi routers <wsgi-routing>`:

* A :class:`.Router` to render the web page
* A :class:`.WebSocket` with the :class:`Chat` handler
* A :class:`.Router` with the :class:`Rpc` handler
  for exposing a :ref:`JSON-RPC <apps-rpc>` api.

To run the server::

    python manage.py

and open web browsers at http://localhost:8060

To send messages from the JSON RPC open a python shell and::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060/rpc')
    >>> p.message('Hi from rpc')
    'OK'

This example uses the pulsar :ref:`Publish/Subscribe handler <apps-pubsub>`
to synchronise messages in a multiprocessing web server.

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
import time

from pulsar import Future, ensure_future
from pulsar.apps.wsgi import (Router, WsgiHandler, LazyWsgi, WSGIServer,
                              GZipMiddleware)
from pulsar.apps.ws import WS, WebSocket
from pulsar.apps.rpc import PulsarServerCommands
from pulsar.apps.data import create_store, PubSubClient
from pulsar.utils.httpurl import JSON_CONTENT_TYPES
from pulsar.apps.ds import pulsards_url
from pulsar.utils.system import json
from pulsar.utils.pep import to_string

CHAT_DIR = os.path.dirname(__file__)


class ChatClient(PubSubClient):
    __slots__ = ('connection', 'channel')

    def __init__(self, connection, channel):
        self.connection = connection
        self.channel = channel

    def __call__(self, channel, message):
        self.connection.write(message)


class Protocol:

    def encode(self, message):
        '''Encode a message when publishing.'''
        if not isinstance(message, dict):
            message = {'message': message}
        message['time'] = time.time()
        return json.dumps(message)

    def decode(self, message):
        return to_string(message)


#    Web Socket Chat handler
class Chat(WS):
    '''The websocket handler (:class:`.WS`) managing the chat application.

    .. attribute:: pubsub

        The :ref:`publish/subscribe handler <apps-pubsub>` created by the wsgi
        application in the :meth:`WebChat.setup` method.
    '''
    def __init__(self, pubsub, channel):
        self.pubsub = pubsub
        self.channel = channel

    def on_open(self, websocket):
        '''When a new websocket connection is established it creates a
        new :class:`ChatClient` and adds it to the set of clients of the
        :attr:`pubsub` handler.'''
        self.pubsub.add_client(ChatClient(websocket, self.channel))

    def on_message(self, websocket, msg):
        '''When a new message arrives, it publishes to all listening clients.
        '''
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                return self.pubsub.publish(self.channel, msg)


#    RPC MIDDLEWARE To publish messages
class Rpc(PulsarServerCommands):

    def __init__(self, pubsub, channel, **kwargs):
        self.pubsub = pubsub
        self.channel = channel
        super().__init__(**kwargs)

    async def rpc_message(self, request, message):
        '''Publish a message via JSON-RPC'''
        await self.pubsub.publish(self.channel, message)
        return 'OK'


class WebChat(LazyWsgi):
    '''This is the :ref:`wsgi application <wsgi-handlers>` for this
    web-chat example.'''
    def __init__(self, server_name):
        self.name = server_name

    def setup(self, environ):
        '''Called once only to setup the WSGI application handler.

        Check :ref:`lazy wsgi handler <wsgi-lazy-handler>`
        section for further information.
        '''
        cfg = environ['pulsar.cfg']
        loop = environ['pulsar.connection']._loop
        self.store = create_store(cfg.data_store, loop=loop)
        pubsub = self.store.pubsub(protocol=Protocol())
        channel = '%s_webchat' % self.name
        ensure_future(pubsub.subscribe(channel), loop=loop)
        return WsgiHandler([Router('/', get=self.home_page),
                            WebSocket('/message', Chat(pubsub, channel)),
                            Router('/rpc', post=Rpc(pubsub, channel),
                                   response_content_types=JSON_CONTENT_TYPES)],
                           [AsyncResponseMiddleware,
                            GZipMiddleware(min_length=20)])

    def home_page(self, request):
        data = open(os.path.join(CHAT_DIR, 'chat.html')).read()
        request.response.content_type = 'text/html'
        request.response.content = to_string(data % request.environ)
        return request.response


def AsyncResponseMiddleware(environ, resp):
    '''This is just for testing the asynchronous response middleware
    '''
    future = Future()
    future._loop.call_soon(future.set_result, resp)
    return future


def server(callable=None, name=None, data_store=None, **params):
    name = name or 'wsgi'
    data_store = pulsards_url(data_store)
    return WSGIServer(callable=WebChat(name), name=name,
                      data_store=data_store, **params)


if __name__ == '__main__':  # pragma nocover
    server().start()
