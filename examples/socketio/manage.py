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

p = lambda x : os.path.split(x)[0]
path = p(p(p(os.path.abspath(__file__))))
if path not in sys.path:
    sys.path.insert(0,path)

import pulsar
from pulsar import net, to_bytestring
from pulsar.apps import ws, wsgi
from pulsar.utils.py2py3 import range


class handle(ws.WS):
    
    def on_message(self, msg):
        path = self.environ['PATH_INFO']
        if path == '/echo':
            self.write_message(msg)
                
        elif path == '/data':
            data = [(i,random()) for i in range(100)]
            self.write_message(json.dumps(data))


def page(environ, start_response):
    """ This resolves to the web page or the websocket depending on the path."""
    path = environ['PATH_INFO']
    if not path or path == '/':
        data = open(os.path.join(os.path.dirname(__file__), 
                     'socketio.html')).read()
        data = data % environ
        start_response('200 OK', [('Content-Type', 'text/html'),
                                  ('Content-Length', str(len(data)))])
        return [to_bytestring(data)]



def server(**kwargs):
    app = wsgi.WsgiHandler(middleware = (page,
                                         ws.SocketIOMiddleware(handle)))
    return wsgi.createServer(callable = app,
                             **kwargs)


if __name__ == '__main__':
    server().start()
