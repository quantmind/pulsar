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


class Graph(ws.WS):
    
    def on_message(self, protocol, msg):
        return json.dumps([(i,random()) for i in range(100)])
    
class Echo(ws.WS):
    
    def on_message(self, protocol, msg):
        return msg


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


class Site(wsgi.LazyWsgi):
    
    def setup(self):
        return wsgi.WsgiHandler(middleware=(page,
                                            ws.WebSocket('/data', Graph()),
                                            ws.WebSocket('/echo', Echo())))


def server(**kwargs):
    return wsgi.WSGIServer(callable=Site(), **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()
