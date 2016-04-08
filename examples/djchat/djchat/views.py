import time

from pulsar import HttpException
from pulsar.apps import ws
from pulsar.apps.data import PubSubClient, create_store
from pulsar.utils.system import json
from pulsar.utils.string import random_string


def home(request):
    from django.shortcuts import render_to_response
    return render_to_response('home.html', {
        'HOST': request.get_host()
        })


class ChatClient(PubSubClient):

    def __init__(self, websocket):
        self.joined = time.time()
        self.websocket = websocket
        self.websocket._chat_client = self

    def __call__(self, channel, message):
        # The message is an encoded JSON string
        self.websocket.write(message, opcode=1)


class Chat(ws.WS):
    ''':class:`.WS` handler managing the chat application.'''
    _store = None
    _pubsub = None
    _client = None

    async def get_pubsub(self, websocket):
        '''Create the pubsub handler if not already available'''
        if not self._store:
            cfg = websocket.cfg
            self._store = create_store(cfg.data_store)
            self._client = self._store.client()
            self._pubsub = self._store.pubsub()
            webchat = '%s:webchat' % cfg.exc_id
            chatuser = '%s:chatuser' % cfg.exc_id
            await self._pubsub.subscribe(webchat, chatuser)
        return self._pubsub

    async def on_open(self, websocket):
        '''A new websocket connection is established.

        Add it to the set of clients listening for messages.
        '''
        pubsub = await self.get_pubsub(websocket)
        pubsub.add_client(ChatClient(websocket))
        user, _ = self.user(websocket)
        users_key = 'webchatusers:%s' % websocket.cfg.exc_id
        # add counter to users
        registered = await self._client.hincrby(users_key, user, 1)
        if registered == 1:
            await self.publish(websocket, 'chatuser', 'joined')

    async def on_close(self, websocket):
        '''Leave the chat room
        '''
        user, _ = self.user(websocket)
        users_key = 'webchatusers:%s' % websocket.cfg.exc_id
        registered = await self._client.hincrby(users_key, user, -1)
        pubsub = await self.get_pubsub(websocket)
        pubsub.remove_client(websocket._chat_client)
        if not registered:
            await self.publish(websocket, 'chatuser', 'gone')
        if registered <= 0:
            await self._client.hdel(users_key, user)

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
                return self.publish(websocket, 'webchat', msg)

    def user(self, websocket):
        user = websocket.handshake.get('django.user')
        if user.is_authenticated():
            return user.username, True
        else:
            session = websocket.handshake.get('django.session')
            user = session.get('chatuser')
            if not user:
                user = 'an_%s' % random_string(length=6).lower()
                session['chatuser'] = user
            return user, False

    def publish(self, websocket, channel, message=''):
        user, authenticated = self.user(websocket)
        msg = {'message': message,
               'user': user,
               'authenticated': authenticated,
               'channel': channel}
        channel = '%s:%s' % (websocket.cfg.exc_id, channel)
        return self._pubsub.publish(channel, json.dumps(msg))


class middleware:
    '''Django middleware for serving the Chat websocket.'''
    def __init__(self):
        self._web_socket = ws.WebSocket('/message', Chat())

    def process_request(self, request):
        from django.http import HttpResponse
        environ = request.META
        environ['django.user'] = request.user
        environ['django.session'] = request.session
        try:
            response = self._web_socket(environ)
        except HttpException as e:
            return HttpResponse(status=e.status)
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
            environ.pop('django.session')
