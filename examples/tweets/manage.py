'''
This example uses the streaming capabilities of the :class:`.HttpClient`
to hook into twitter streaming api and send tweets into a processing queue.

Implementation
====================

.. autoclass:: Twitter
   :members:
   :member-order: bysource
'''
from os import path

import pulsar
from pulsar.utils.system import json
from pulsar.apps.data import create_store
from pulsar.apps.ds import pulsards_url
from pulsar.apps.wsgi import (WSGIServer, LazyWsgi, WsgiHandler, Router,
                              FileRouter)
from pulsar.apps.ws import WebSocket, PubSubWS
from pulsar.apps.http import HttpClient, OAuth1

STATIC_DIR = path.join(path.dirname(__file__), 'assets')
FAVICON = path.join(STATIC_DIR, 'favicon.ico')


class Twitter(pulsar.Application):
    '''This applications requires the following parameters
    to be specified in your ``config.py`` file:

    * ``twitter_api_key`` the Consumer key of your application
    * ``twitter_api_secret``, the Consumer secret
    * ``twitter_access_token``, the application Access token
    * ``twitter_access_secret``, the Access token secret
    * ``twitter_stream_filter``, dictionary of parameters for
      filtering tweets.

    Check the twitter stream filter api for further information

    https://dev.twitter.com/docs/api/1.1/post/statuses/filter
    '''
    interval1 = 0
    interval2 = 0
    interval3 = 0
    public_stream = 'https://stream.twitter.com/1.1/statuses/filter.json'

    def monitor_start(self, monitor):
        # this application has no workers
        self.cfg.set('workers', 0)
        api_key = self.get_param('twitter_api_key')
        client_secret = self.get_param('twitter_api_secret')
        access_token = self.get_param('twitter_access_token')
        access_secret = self.get_param('twitter_access_secret')
        self._http = HttpClient(encode_multipart=False)
        oauth2 = OAuth1(api_key,
                        client_secret=client_secret,
                        resource_owner_key=access_token,
                        resource_owner_secret=access_secret)
        self._http.bind_event('pre_request', oauth2)
        self.buffer = []
        self.connect()

    def connect(self):
        filter = self.get_param('twitter_stream_filter')
        self._http.post(self.public_stream, data=filter,
                        on_headers=self.connected,
                        data_processed=self.process_data,
                        post_request=self.reconnect)

    def connected(self, response, **kw):
        if response.status_code == 200:
            self.logger.info('Successfully connected with twitter streaming')
            self.interval1 = 0
            self.interval2 = 0
            self.interval3 = 0

    def process_data(self, response, **kw):
        if response.status_code == 200:
            messages = []
            data = response.recv_body()
            while data:
                idx = data.find(b'\r\n')
                if idx < 0:
                    self.buffer.append(data)
                    data = None
                else:
                    self.buffer.append(data[:idx])
                    data = data[idx+2:]
                    msg = b''.join(self.buffer)
                    self.buffer = []
                    if msg:
                        body = json.loads(msg.decode('utf-8'))
                        if 'disconnect' in body:
                            msg = body['disconnect']
                            self.logger.warning('Disconnecting (%d): %s',
                                                msg['code'], msg['reason'])
                        elif 'warning' in body:
                            message = body['warning']['message']
                            self.logger.warning(message)
                        else:
                            messages.append(body)
            if messages:
                # a list of messages is available
                if self.cfg.callable:
                    self.cfg.callable(self, messages)

    def reconnect(self, response, exc=None):
        '''Handle reconnection according to twitter streaming policy

        https://dev.twitter.com/docs/streaming-apis/connecting
        '''
        loop = response._loop
        if response.status_code == 200:
            gap = 0
        elif not response.status_code:
            # This is a network error, back off lineraly 250ms up to 16s
            self.interval1 = gap = max(self.interval1+0.25, 16)
        elif response.status_code == 420:
            gap = 60 if not self.interval2 else max(2*self.interval2)
            self.interval2 = gap
        else:
            gap = 5 if not self.interval3 else max(2*self.interval3, 320)
            self.interval3 = gap
        loop.call_later(gap, self.connect)

    def get_param(self, name):
        value = self.cfg.get(name)
        if not value:
            raise pulsar.ImproperlyConfigured(
                'Please specify the "%s" parameter in your %s file' %
                (name, self.cfg.config))
        return value


class ProcessTweets:
    '''Tweets processor

    This callable class is passed to the :class:`.Twitter` application
    and it is invoked every time new messages are available.
    '''
    store = None

    def __init__(self, channel):
        self.channel = channel

    def __call__(self, twitter, messages):
        if not self.store:
            self.store = create_store(twitter.cfg.data_store)
            self.pubsub = self.store.pubsub()
        for message in messages:
            self.pubsub.publish(self.channel, json.dumps(message))


class TweetsWsHandler(PubSubWS):
    '''WebSocket Handler for new tweets
    '''
    def on_message(self, websocket, msg):
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                self.pubsub.publish(self.channel, msg)


class Site(LazyWsgi):
    '''A simple web site for displaying tweets
    '''
    def __init__(self, channel):
        self.channel = channel

    def setup(self, environ):
        cfg = environ['pulsar.cfg']
        self.store = create_store(cfg.data_store)
        pubsub = self.store.pubsub()
        return WsgiHandler([Router('/', get=self.home_page),
                            FileRouter('/favicon.ico', FAVICON),
                            WebSocket('/message',
                                      TweetsWsHandler(pubsub, self.channel))])

    def home_page(self, request):
        doc = request.html_document
        head = doc.head
        head.title = 'Pulsar Twitter Stream'
        head.links.append('bootstrap_css')
        head.scripts.append('require')
        head.scripts.require('jquery')
        data = open(path.join(STATIC_DIR, 'home.html')).read()
        doc.body.append(data)
        return doc.http_response(request)


class Server(pulsar.MultiApp):
    '''Create two pulsar applications, one for serving the web site and
    one for streaming tweets
    '''
    # set the default data_store to be pulsar
    cfg = {'data_store': pulsards_url()}

    def build(self):
        # the pubsub channel
        channel = '%s_tweets' % self.name
        yield self.new_app(WSGIServer, callable=Site(channel))
        # yield self.new_app(Twitter, callable=ProcessTweets(channel))


if __name__ == '__main__':  # pragma nocover
    Server().start()
