'''Simple chat application.

    python manage.py
    
and open web browsers at http://localhost:8060

To send messages from the JSON RPC open a python shell and::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060/rpc')
    >>> p.message('Hi from rpc')
    'OK'
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
from pulsar.apps import ws, wsgi, rpc, pubsub

CHAT_DIR = os.path.dirname(__file__)

##    RPC MIDDLEWARE To publish messages
class Rpc(rpc.PulsarServerCommands):
    
    def rpc_message(self, request, message):
        pubsub.publish(message)
        return 'OK'
    

##    Web Socket Chat handler
class Chat(ws.WS):
    
    def on_open(self, request):
        # Add pulsar.connection environ extension to the set of active clients
        pubsub.subscribe(request.cache['websocket'])
        
    def on_message(self, request, msg):
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                pubsub.publish(msg)


class WebChat(wsgi.LazyWsgi):
    
    def __init__(self, name):
        self.name = name
        
    def setup(self):
        # Register a pubsub handler
        pubsub.register_handler(pubsub.PulsarPubSub(self.name))
        return wsgi.WsgiHandler([wsgi.Router('/', get=self.home_page),
                                 ws.WebSocket('/message', Chat()),
                                 wsgi.Router('/rpc', post=Rpc())])
        
    def home_page(self, request):
        data = open(os.path.join(CHAT_DIR, 'chat.html')).read()
        request.response.content_type = 'text/html'
        request.response.content = data % request.environ
        return request.response.start()


def server(callable=None, name=None, **kwargs):
    name = name or 'webchat'
    chat = WebChat(name)
    return wsgi.WSGIServer(name=name, callable=chat, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()
