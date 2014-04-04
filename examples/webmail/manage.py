'''The example is a :ref:`WSGI application <apps-wsgi>`
with a :ref:`websocket middleware <apps-ws>` which connects to an
IMAP4 server to retrieve and send emails.

The connection with the IMAP4 server is obtained using the IMAP4 API in
twisted 12.3 or later. The example uses
:ref:`pulsar-twisted integration <tutorials-twisted>` module.

To run the server you need to create a :mod:`config.py` file in the
the :mod:`examples.webmail` directory containing::

    # the adress of your mail server
    mail_incoming ='ssl:host=imap.gmail.com:port=993'
    # mail_username & mail_password
    mail_username=
    mail_password=

And type::

    python manage.py

Open a web browser at http://localhost:8060 and you should see the web app.

For information on twisted IMAP4 client library check this example:

http://twistedmatrix.com/documents/current/mail/examples/imap4client.py

Other python examples of webmail:

* https://github.com/khamidou/kite

Implementation
==================

.. autoclass:: WsMail
   :members:
   :member-order: bysource

'''
import os
import sys

from pulsar import coroutine_return
from pulsar.apps import ws, wsgi
from pulsar.utils.log import process_global
from pulsar.utils.system import json
try:
    from pulsar.apps.tx import twisted
    from twisted.internet import protocol, endpoints, reactor
    from twisted.mail import imap4
except ImportError:  # pragma    nocover
    twisted = None    # This is for when we build docs

ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')


def mail_client(cfg, timeout=10):
    '''Create a new mail client using twisted IMAP4 library.'''
    key = (cfg.mail_incoming, cfg.mail_username, cfg.mail_password)
    client = process_global(key)
    if not client:
        endpoint = endpoints.clientFromString(reactor, cfg.mail_incoming)
        endpoint._timeout = timeout
        factory = protocol.Factory()
        factory.protocol = imap4.IMAP4Client
        client = yield endpoint.connect(factory)
        yield client.login(cfg.mail_username, cfg.mail_password)
        yield client.select('INBOX')
        process_global(key, client, True)
    coroutine_return(client)
    # info = yield client.fetchEnvelope(imap4.MessageSet(1))
    # print 'First message subject:', info[1]['ENVELOPE'][1]


class WsMail(ws.WS):
    '''A :class:`.WS` handler for fetching and sending mail via the
    twisted IMAP4 library
    '''
    def on_open(self, websocket):
        '''When the websocket starts, it create a new mail client.'''
        request = websocket.handshake
        client = yield mail_client(request.cfg)
        # add the mail client to the environ cache
        request.cache.mailclient = client
        # retrieve the list of mailboxes and them to the client
        yield self._send_mailboxes(websocket)

    def on_message(self, websocket, msg):
        request = websocket.request
        client = request.cache.mailclient
        if msg and client:
            msg = json.loads(msg)
            if 'mailbox' in msg:
                mailbox = yield client.examine(msg['mailbox'])
                self.write(request, json.dumps({'mailbox': mailbox}))

    def _send_mailboxes(self, websocket):
        request = websocket.handshake
        result = yield request.cache.mailclient.list("", "*")
        result = sorted([e[2] for e in result])
        websocket.write(json.dumps({'list': result}))


class WebMail(wsgi.LazyWsgi):

    def setup(self, environ):
        return wsgi.WsgiHandler([ws.WebSocket('/message', WsMail()),
                                 wsgi.MediaRouter('/media', ASSET_DIR),
                                 wsgi.Router('/', get=self.home)])

    def home(self, request):
        data = open(os.path.join(ASSET_DIR, 'mail.html')).read()
        response = request.response
        request.response.content = data % request.environ
        request.response.content_type = 'text/html'
        return response


def server(name='webmail', callable=None, **kwargs):
    return wsgi.WSGIServer(name=name, callable=WebMail(), **kwargs)


if __name__ == '__main__':  # pragma nocover
    server().start()
