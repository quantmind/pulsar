'''\
A very Simple Web-Socket example.
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
from pulsar.utils.httpurl import range


class handle(ws.WS):
    
    def match(self, environ):
        return environ.get('PATH_INFO') in ('/echo', '/data')
    
    def on_message(self, environ, msg):
        path = environ.get('PATH_INFO')
        if path == '/echo':
            return msg
        elif path == '/data':
            return json.dumps([(i,random()) for i in range(100)])


def page(environ, start_response):
    """ This resolves to the web page or the websocket depending on the path."""
    path = environ.get('PATH_INFO')
    if not path or path == '/':
        data = open(os.path.join(os.path.dirname(__file__), 
                     'websocket.html')).read()
        data = data % environ
        start_response('200 OK', [('Content-Type', 'text/html'),
                                  ('Content-Length', str(len(data)))])
        return [pulsar.to_bytes(data)]



def server(**kwargs):
    app = wsgi.WsgiHandler(middleware=(page, ws.WebSocket(handle())))
    return wsgi.WSGIServer(callable=app, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()
