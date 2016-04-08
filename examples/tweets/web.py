'''
.. autoclass:: TwitterSite
   :members:
   :member-order: bysource

'''
import os
import json
from string import Template

from pulsar import ensure_future
from pulsar.apps.data import create_store
from pulsar.apps.wsgi import LazyWsgi, WsgiHandler, Router, MediaRouter
from pulsar.apps.ws import WebSocket, PubSubWS

STATIC_DIR = os.path.join(os.path.dirname(__file__), 'assets')
DESCRIPTION = ('An example of using pulsar with two actor monitors, one '
               'for a WSGI server and one for a Twitter streaming server')


class TweetsWsHandler(PubSubWS):
    '''WebSocket Handler for new tweets
    '''
    def write(self, websocket, message):
        message = message.decode('utf-8')
        super().write(websocket, message)


class TwitterSite(LazyWsgi):
    '''A :class:`.LazyWsgi` handler for displaying tweets
    '''
    def __init__(self, channel):
        self.channel = channel

    def setup(self, environ):
        '''Called once only by the WSGI server.

        It returns a :class:`.WsgiHandler` with three routes:

        * The base route served by the :meth:`home_page` method
        * The websocket route
        * A route for static files
        '''
        cfg = environ['pulsar.cfg']
        # Create the store and the pubsub handler
        self.store = create_store(cfg.data_store)
        pubsub = self.store.pubsub()
        # subscribe to channel
        ensure_future(self.subscribe(pubsub))
        return WsgiHandler([Router('/', get=self.home_page),
                            MediaRouter('/static', STATIC_DIR),
                            WebSocket('/message',
                                      TweetsWsHandler(pubsub, self.channel))])

    def home_page(self, request):
        '''Renders the home page
        '''
        doc = request.html_document
        self.doc_head(request)
        html = Template(open(os.path.join(STATIC_DIR, 'home.html')).read())
        scheme = 'wss' if request.is_secure else 'ws'
        ws = request.absolute_uri('/message', scheme=scheme)
        html = html.safe_substitute({'twitter': json.dumps({'url': ws})})
        doc.body.append(html)
        return doc.http_response(request)

    def doc_head(self, request, title=None, description=None):
        cfg = request.cfg
        description = description or DESCRIPTION

        head = request.html_document.head
        head.title = title or 'Pulsar Twitter Stream'
        head.add_meta(name='description', content=description)
        head.links.append('/static/favicon.ico', rel="icon",
                          type='image/x-icon')
        head.links.append(cfg.BOOTSTRAP)
        head.scripts.append(cfg.REQUIRE_JS)
        head.scripts.append('/static/tweets.js')

    def subscribe(self, pubsub):
        '''Subscribe to the channel for tweets
        '''
        return pubsub.subscribe(self.channel)
