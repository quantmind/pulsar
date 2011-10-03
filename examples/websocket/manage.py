'''\
A a Simple Web-Socket example.
To run the server type::

    python manage.py
    
and open a web browser at http://localhost:8060    
'''
import os
import json
from random import random
import time

import pulsar
from pulsar import net
from pulsar.utils.py2py3 import range


class handle(net.WS):
    
    def on_message(self, msg):
        path = self.environ['REQUEST_PATH']
        if path == '/echo':
            self.write_message(msg)
                
        elif path == '/data':
            data = [(i,random()) for i in range(10000)]
            self.write_message(json.dumps(data))


def page(environ, start_response):
    """ This resolves to the web page or the websocket depending on the path."""
    path = environ['PATH_INFO']
    if not path or path == '/':
        data = open(os.path.join(os.path.dirname(__file__), 
                     'websocket.html')).read()
        data = data % environ
        start_response('200 OK', [('Content-Type', 'text/html'),
                                  ('Content-Length', str(len(data)))])
        return [data]


app = net.WsgiHandler(\
        middleware = (page,
                      net.WebSocket(handle)))


def server(**kwargs):
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(callable = app,
                             **kwargs)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()
