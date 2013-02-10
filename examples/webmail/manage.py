'''A webmail application to demonstrate pulsar-twisted integration.
To run the server type::

    python manage.py
    
and open a web browser at http://localhost:8060    
'''
import os
import sys
from functools import partial
import time
try:
    import pulsar
except ImportError: #pragma nocover
    sys.path.append('../../')
    import pulsar
from pulsar.apps import ws, wsgi, rpc
from pulsar.lib.tx import twisted
from twisted.internet import protocol, defer, endpoints, task, reactor
from twisted.mail import imap4

try:
    import config
except ImportError:
    print('Create a config.py file with connection_string, username and '
          'password which will be used to connect to your inbox')
    exit(0)
    
@pulsar.async(1)
def mail_client():
    endpoint = endpoints.clientFromString(reactor, config.incoming_mail)
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

THIS_DIR = os.path.dirname(__file__)
    
class Mail(ws.WS):
        
    def on_open(self, environ):
        # Add pulsar.connection environ extension to the set of active clients
        return mail_client().add_callback(partial(self._on_open, environ))
                            
        
    def _on_open(self, environ, client):
        environ['mail.client'] = client
        
    def on_message(self, environ, msg):
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                publish(msg)


class Web(wsgi.Router):

    def post(self, request):
        pass
    
    def get(self, request):
        """ This resolves to the web page or the websocket depending on the path."""
        data = open(os.path.join(THIS_DIR, 'mail.html')).read()
        request.response.content = data % request.environ
        request.response.content_type = 'text/html'
        return request.response.start()



def server(**kwargs):
    web = Web('/')
    mail = ws.WebSocket('/message', Mail())
    middleware = wsgi.WsgiHandler(middleware=(mail, web))
    return wsgi.WSGIServer(name='webmail', callable=middleware, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()