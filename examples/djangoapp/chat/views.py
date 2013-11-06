import time

from pulsar import is_failure, get_actor
from pulsar.apps import ws, pubsub
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.log import lazyproperty
from pulsar.utils.system import json


def home(request):
    from django.shortcuts import render_to_response
    from django.template import RequestContext
    return render_to_response('home.html', {
        'HOST': request.get_host()
        }, RequestContext(request))


class Client(pubsub.Client):

    def __init__(self, connection):
        self.connection = connection

    def __call__(self, channel, message):
        if channel == 'webchat':
            self.connection.write(message)


class Chat(ws.WS):
    '''The websocket handler managing the chat application.
    '''
    _pubsub = None

    def pubsub(self, websocket):
        if not self._pubsub:
            # ``pulsar.cfg`` is injected by the pulsar server into
            # the wsgi environ. Here we pick up the name of the wsgi
            # application running the server. This is **only** needed by the
            # test suite which tests several servers/clients at once.
            name = websocket.handshake.environ['pulsar.cfg'].name
            self._pubsub = pubsub.PubSub(name=name)
            self._pubsub.subscribe('webchat')
        return self._pubsub

    def on_open(self, websocket):
        '''A new websocket connection is established.

        Add it to the set of clients listening for messages.
        '''
        self.pubsub(websocket).add_client(Client(websocket))

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
                user = websocket.handshake.get('django.user')
                if user.is_authenticated():
                    user = user.username
                else:
                    user = 'anonymous'
                msg = {'message': msg, 'user': user, 'time': time.time()}
                self.pubsub(websocket).publish('webchat', json.dumps(msg))


class middleware(object):
    '''Django middleware for serving the Chat websocket.'''
    def __init__(self):
        self._web_socket = ws.WebSocket('/message', Chat())

    def process_request(self, request):
        from django.http import HttpResponse
        environ = request.META
        environ['django.user'] = request.user
        response = self._web_socket(environ)
        if response is not None:
            # we have a response, this is the websocket upgrade.
            # Convert to django response
            resp = HttpResponse(status=response.status_code,
                                content_type=response.content_type)
            for header, value in response.headers:
                resp[header] = value
            return resp
        else:
            environ.pop('django.user')
