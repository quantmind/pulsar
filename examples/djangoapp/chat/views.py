import time
from hashlib import sha1

from pulsar import get_actor, coroutine_return
from pulsar.apps import ws
from pulsar.apps.data import PubSubClient, create_store
from pulsar.utils.log import lazyproperty
from pulsar.utils.system import json


def home(request):
    from django.shortcuts import render_to_response
    from django.template import RequestContext
    return render_to_response('home.html', {
        'HOST': request.get_host()
        }, RequestContext(request))


class ChatClient(PubSubClient):

    def __init__(self, websocket):
        self.joined = time.time()
        self.websocket = websocket

    def __call__(self, channel, message):
        # The message is an encoded JSON string
        self.websocket.write(message, opcode=1)


class Chat(ws.WS):
    ''':class:`.WS` handler managing the chat application.'''
    _store = None
    _pubsub = None
    _client = None

    def get_pubsub(self, websocket):
        '''Create the pubsub handler if not already available'''
        if not self._store:
            cfg = websocket.cfg
            self._store = create_store(cfg.data_store)
            self._client = self._store.client()
            self._pubsub = self._store.pubsub()
            webchat = '%s:webchat' % cfg.exc_id
            chatuser = '%s:chatuser' % cfg.exc_id
            self._pubsub.subscribe(webchat, chatuser)
        return self._pubsub

    def on_open(self, websocket):
        '''A new websocket connection is established.

        Add it to the set of clients listening for messages.
        '''
        self.get_pubsub(websocket).add_client(ChatClient(websocket))
        user, _ = self.user(websocket)
        users_key = 'webchatusers:%s' % websocket.cfg.exc_id
        # add counter to users
        registered = yield self._client.hincrby(users_key, user, 1)
        if registered == 1:
            self.publish(websocket, 'chatuser', 'joined')

    def on_close(self, websocket):
        '''Leave the chat room
        '''
        user, _ = self.user(websocket)
        users_key = 'webchatusers:%s' % websocket.cfg.exc_id
        registered = yield self._client.hincrby(users_key, user, -1)
        if not registered:
            self.publish(websocket, 'chatuser', 'gone')
        if registered <= 0:
            self._client.hdel(users_key, user)

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
        if isinstance(user, str):
            return user, False
        elif user.is_authenticated():
            return user.username, True
        else:
            ip = websocket.handshake.ipaddress
            ipsha = sha1(ip.encode('utf-8')).hexdigest()[:6]
            user = 'an_%s' % ipsha
            websocket.handshake.environ['django.user'] = user
            return user, False

    def publish(self, websocket, channel, message=''):
        user, authenticated = self.user(websocket)
        msg = {'message': message,
               'user': user,
               'authenticated': authenticated,
               'channel': channel}
        channel = '%s:%s' % (websocket.cfg.exc_id, channel)
        return self._pubsub.publish(channel, json.dumps(msg))


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
