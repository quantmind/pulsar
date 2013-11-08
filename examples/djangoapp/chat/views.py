import time

from pulsar import get_actor, coroutine_return
from pulsar.apps import ws
from pulsar.apps.data import PubSubClient, start_store
from pulsar.utils.log import lazyproperty
from pulsar.utils.system import json


def home(request):
    from django.shortcuts import render_to_response
    from django.template import RequestContext
    return render_to_response('home.html', {
        'HOST': request.get_host()
        }, RequestContext(request))


class ChatClient(PubSubClient):

    def __init__(self, connection):
        self.joined = time.time()
        self.connection = connection

    def __call__(self, channel, message):
        self.connection.write(message)


class Chat(ws.WS):
    ''':class:`.WS` handler managing the chat application.'''
    pubsub = None

    def get_pubsub(self, websocket):
        if not self.pubsub:
            # ``pulsar.cfg`` is injected by the pulsar server into
            # the wsgi environ.
            cfg = websocket.handshake.environ['pulsar.cfg']
            data_server_dns = cfg.pubsub_server or 'pulsar://127.0.0.1:0'
            store = yield start_store(data_server_dns, loop=websocket._loop)
            self.pubsub = store.pubsub()
            yield self.pubsub.subscribe('webchat', 'chatuser')
        coroutine_return(self.pubsub)

    def on_open(self, websocket):
        '''A new websocket connection is established.

        Add it to the set of clients listening for messages.
        '''
        pubsub = self.pubsub
        if not pubsub:
            pubsub = yield self.get_pubsub(websocket)
        pubsub.add_client(ChatClient(websocket))
        self.publish(websocket, 'chatuser')
        self.publish(websocket, 'webchat', 'joined the chat')

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
                self.publish(websocket, 'webchat', msg)

    def user(self, websocket):
        user = websocket.handshake.get('django.user')
        if user.is_authenticated():
            return user.username
        else:
            return 'anonymous'

    def publish(self, websocket, channel, message=''):
        msg = {'message': message,
               'user': self.user(websocket),
               'channel': channel,
               'time': time.time()}
        return self.pubsub.publish(channel, json.dumps(msg))


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
