'''A websocket chat application.
To run the server type::

    python manage.py
    
and open a web browser at http://localhost:8060    
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
def publish(message):
    message = {'time': time.time(),
               'message': message}
    pulsar.send('webchat', 'publish_message', json.dumps(message))

def broadcast(message):
    remove = set()
    clients = get_clients()
    for client in clients:
        try:
            client.current_consumer.write(message)
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
def publish_message(request, message):
    monitor = request.actor
    if monitor.managed_actors:
        for worker in monitor.managed_actors.values():
            monitor.send(worker, 'broadcast_message', message)
    else:
        broadcast(message)
        
@pulsar.command(ack=False)
def broadcast_message(request, message):
    broadcast(message)

################################################################################
##    MIDDLEWARE

class Rpc(rpc.PulsarServerCommands):
    
    def rpc_message(self, request, message):
        publish(message)
        return 'OK'
    
    
class Chat(ws.WS):
        
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
    chat = ws.WebSocket('/message', Chat())
    api = rpc.RpcMiddleware(Rpc(), path='/rpc')
    middleware = wsgi.WsgiHandler(middleware=(chat, api, page))
    return wsgi.WSGIServer(name='webchat', callable=middleware, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()