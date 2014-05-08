'''
This example uses the streaming capabilities of pulsar :class:`.HttpClient`
to hook into twitter streaming api and send tweets into a processing queue.
'''
import pulsar
from pulsar.utils.system import json
from pulsar.apps.wsgi import WSGIServer, LazyWsgi
from pulsar.apps.http import HttpClient, OAuth1


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
    cfg = {'workers': 0}
    public_stream = 'https://stream.twitter.com/1.1/statuses/filter.json'

    def monitor_start(self, monitor):
        api_key = self.get_param('twitter_api_key')
        client_secret = self.get_param('twitter_api_secret')
        access_token = self.get_param('twitter_access_token')
        access_secret = self.get_param('twitter_access_secret')
        filter = self.get_param('twitter_stream_filter')
        self._http = HttpClient(encode_multipart=False)
        oauth2 = OAuth1(api_key,
                        client_secret=client_secret,
                        resource_owner_key=access_token,
                        resource_owner_secret=access_secret)
        self._http.bind_event('pre_request', oauth2)
        self.buffer = []
        response = yield self._http.post(self.public_stream, data=filter,
                                         data_processed=self.process_data)
        a = 1

    def process_data(self, response, **kw):
        '''Process twitter streaming data
        '''
        if response.status_code == 200:
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
                            self.handle_disconnect()
                        elif 'warning' in body:
                            message = body['warning']['message']
                            self.logger.warning(message)
                        else:
                            print(msg)


    def get_param(self, name):
        value = self.cfg.get(name)
        if not value:
            raise pulsar.ImproperlyConfigured(
                'Please specify the "%s" parameter in your %s file' %
                (name, self.cfg.config))
        return value

class Site(LazyWsgi):

    def setup(self, environ):
        pass


class Server(pulsar.MultiApp):
    '''Create two pulsar applications, one for serving the web site and
    one for streaming tweets
    '''
    def build(self):
        yield self.new_app(WSGIServer, callable=Site())
        yield self.new_app(Twitter)


if __name__ == '__main__':  # pragma nocover
    Server().start()
