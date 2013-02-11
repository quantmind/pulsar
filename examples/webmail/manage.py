'''A webmail application to demonstrate pulsar-twisted integration.
To run the server you need to create a config.py file containing::

    incoming_mail='ssl:host=imap.gmail.com:port=993'
    username=
    password=

type::

    python manage.py
    
and open a web browser at http://localhost:8060

Adapted from this example

http://twistedmatrix.com/documents/current/mail/examples/imap4client.py
'''
import os
import sys
import json
from functools import partial
import time
try:
    import pulsar
except ImportError: #pragma nocover
    sys.path.append('../../')
    import pulsar
from pulsar.utils.pep import zip
from pulsar.apps import ws, wsgi
from pulsar.lib.tx import twisted
from twisted.internet import protocol, defer, endpoints, reactor
from twisted.mail import imap4

try:
    import config
except ImportError:
    print('Create a config.py file with connection_string, username and '
          'password which will be used to connect to your inbox')
    exit(0)

THIS_DIR = os.path.dirname(__file__)

    
@pulsar.async(1)
def mail_client(timeout=10):
    endpoint = endpoints.clientFromString(reactor, config.incoming_mail)
    endpoint._timeout = timeout
    factory = protocol.Factory()
    factory.protocol = imap4.IMAP4Client
    future = endpoint.connect(factory)
    yield future
    client = future.result
    yield client.login(config.username, config.password)
    yield client.select('INBOX')
    yield client
    #info = yield client.fetchEnvelope(imap4.MessageSet(1))
    #print 'First message subject:', info[1]['ENVELOPE'][1]


class Mail(ws.WS):
    '''Websocket handler for fetching and sending mail'''    
    def on_open(self, environ):
        # Add pulsar.connection environ extension to the set of active clients
        return mail_client().add_callback(partial(self._on_open, environ))
        
    def on_message(self, environ, msg):
        client = environ.get('mail.client')
        if msg and client:
            msg = json.loads(msg)
            if 'mailbox' in msg:
                mailbox = msg[mailbox]
                future = client.examine(mailbox)
                yield future
                result = future.result
                self.write(environ, json.dumps({'mailbox': result}))

    def _on_open(self, environ, client):
        environ['mail.client'] = client
        future = client.list("","*")
        yield future
        result = sorted([e[2] for e in future.result])
        self.write(environ, json.dumps({'list': result}))


class Web(wsgi.Router):
    '''Main web page'''
    def post(self, request):
        pass
    
    def get(self, request):
        """ This resolves to the web page or the websocket depending on the path."""
        data = open(os.path.join(THIS_DIR, 'mail.html')).read()
        request.response.content = data % request.environ
        request.response.content_type = 'text/html'
        return request.response.start()


def server(**kwargs):
    mail = ws.WebSocket('/message', Mail())
    media = wsgi.MediaRouter('/media', THIS_DIR)
    web = Web('/')
    middleware = wsgi.WsgiHandler(middleware=(media, mail, web))
    return wsgi.WSGIServer(name='webmail', callable=middleware, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()