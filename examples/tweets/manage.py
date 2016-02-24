import asyncio

import pulsar
from pulsar.apps.wsgi import WSGIServer
from pulsar.apps.ds import pulsards_url

from web import TwitterSite
from twitter import Twitter, PublishTweets


class Server(pulsar.MultiApp):
    '''Create two pulsar applications,

    one for serving the web site and one for streaming tweets
    '''
    # set the default data_store to be pulsar
    cfg = {'data_store': pulsards_url()}

    @asyncio.coroutine
    def build(self):
        # the pubsub channel
        channel = '%s_tweets' % self.name
        yield self.new_app(WSGIServer, callable=TwitterSite(channel))
        yield self.new_app(Twitter, callable=PublishTweets(channel))


if __name__ == '__main__':  # pragma nocover
    Server().start()
