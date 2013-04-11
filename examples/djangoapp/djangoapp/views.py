from django.conf import settings
from django.shortcuts import render_to_response
from django.template import RequestContext

from pulsar.apps import wsgi, ws, pubsub
from pulsar.utils.structures import AttributeDictionary


def home(request):
    return render_to_response('home.html', {
        'HOST': request.environ['HTTP_HOST']
        }, RequestContext(request))



##    Web Socket Chat handler
class Chat(ws.WS):
    '''The websocket handler (:class:`pulsar.apps.ws.WS`) managing the chat
application.'''
    def on_open(self, request):
        '''When a new websocket connection is established it add connection
to the set of clients listening for messages.'''
        pubsub.add_client(request.cache['websocket'])
        
    def on_message(self, request, msg):
        '''When a new message arrives, it publishes to all listening clients.'''
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                user = request.get('django.cache').user
                if user.is_authenticated():
                    user = user.username
                else:
                    user = 'anonymous'
                msg = {'message': msg, 'user': user}
                pubsub.publish(msg)
                
                
class middleware(object):
    '''Middleware for serving the Chat websocket'''
    def __init__(self):
        pubsub.register_handler(pubsub.PulsarPubSub('pulsar_django'))
        self._web_socket = ws.WebSocket('/message', Chat())
        
    def process_request(self, request):
        data = AttributeDictionary(request.__dict__)
        environ = data.pop('environ')
        environ['django.cache'] = data
        return self._web_socket(environ, None)
    