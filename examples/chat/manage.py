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
from pulsar.apps import ws, wsgi, rpc

CHAT_DIR = os.path.dirname(__file__)

################################################################################
##    Publish and clients methods
wsframe = lambda msg: ws.Frame(msg, final=True).msg

def publish(message):
    actor = pulsar.get_actor()
    actor.send('wsgi', 'publish_message', message)
    # This will change in pulsar 0.5 to simply
    # send('monitor', 'message_arrived', message)

def broadcast(message):
    remove = set()
    clients = get_clients()
    for client in clients:
        try:
            client.write(wsframe(message))
        except Exception:
            remove.add(client)
    clients.difference_update(remove)
    
def get_clients():
    actor = pulsar.get_actor()
    clients = actor.params.clients
    if clients is None:
        clients = set()
        actor.params.clients = clients
    return clients

################################################################################
##    Internal message passing
@pulsar.command(ack=False)
def publish_message(client, monitor, message):
    if monitor.managed_actors:
        for worker in monitor.managed_actors.values():
            monitor.send(worker, 'broadcast_message', message)
    else:
        broadcast(message)
        
@pulsar.command(ack=False)
def broadcast_message(client, actor, message):
    broadcast(message)


################################################################################
##    MIDDLEWARE

class Rpc(rpc.PulsarServerCommands):
    
    def rpc_message(self, request, message):
        publish(message)
        return 'OK'
    
    
class Chat(ws.WS):
    
    def match(self, environ):
        return environ.get('PATH_INFO') in ('/message',)
        
    def on_open(self, environ):
        # Add pulsar.connection environ extension to the set of active clients
        get_clients().add(environ['pulsar.connection'])
        
    def on_message(self, environ, msg):
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                publish(msg)


def page(environ, start_response):
    """ This resolves to the web page or the websocket depending on the path."""
    path = environ.get('PATH_INFO')
    if not path or path == '/':
        data = open(os.path.join(CHAT_DIR, 'chat.html')).read()
        data = data % environ
        start_response('200 OK', [('Content-Type', 'text/html'),
                                  ('Content-Length', str(len(data)))])
        return [pulsar.to_bytes(data)]



def server(**kwargs):
    chat = ws.WebSocket(Chat())
    api = rpc.RpcMiddleware(Rpc(), path='/rpc')
    app = wsgi.WsgiHandler(middleware=(chat, api, page))
    return wsgi.WSGIServer(callable=app, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()
