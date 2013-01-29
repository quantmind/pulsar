'''A chat application using redis pubsub
To run the server type::

    python manage.py
    
and open a web browser at http://localhost:8060    
'''
import os
import sys
import json
from random import random
import time
try:
    import pulsar
except ImportError: #pragma nocover
    sys.path.append('../../')
    import pulsar
from pulsar.apps import ws, wsgi
from pulsar.apps.wsgi import Html

CHAT_DIR = os.path.dirname(__file__)
CHAT_URL = 'http://127.0.0.1:8015'

class handle(ws.WS):
    
    def match(self, environ):
        return environ.get('PATH_INFO') in ('/echo', '/data')
    
    def on_message(self, environ, msg):
        path = environ.get('PATH_INFO')
        if path == '/echo':
            return msg
        elif path == '/data':
            return json.dumps([(i,random()) for i in range(100)])


def page(request):
    """ This resolves to the web page or the websocket depending on the path."""
    html = request.html_document(title='Pulsar Chat')
    html.head.scripts.append('http://ajax.googleapis.com/ajax/libs/jquery/1.9.0/jquery.min.js')
    html.head.scripts.append('/media/chat.js')
    html.body.data('chat_url', CHAT_URL)
    return html.http_response(request)



def server(**kwargs):
    websocket = ws.WebSocket(handle())
    static = wsgi.MediaRouter('/media', os.path.join(CHAT_DIR, 'media'))
    main = wsgi.Router('/', get=page)
    app = wsgi.WsgiHandler(middleware=(static, main))
    wsgiserver = wsgi.WSGIServer(callable=app, **kwargs)
    chatserver = wsgi.WSGIServer(callable=websocket, name='chat',
                                 bind=':8015', workers=1)
    wsgiserver.start()


if __name__ == '__main__':  #pragma nocover
    server().start()