'''\
A a Simple Web-Socket example.
To run the server type::

    python manage.py
    
and open a web browser at http://localhost:8060    
'''
import os
import random

import pulsar
from pulsar import http


def handle(ws):
    """This is the websocket handler function.  Note that we 
    can dispatch based on path in here, too."""
    if ws.path == '/echo':
        while True:
            m = ws.wait()
            if m is None:
                break
            ws.send(m)
            
    elif ws.path == '/data':
        for i in xrange(10000):
            ws.send("0 %s %s\n" % (i, random.random()))
            eventlet.sleep(0.1)


wsapp = http.WebSocket(handler = handle)


def app(environ, start_response):
    """ This resolves to the web page or the websocket depending on the path."""
    if environ['PATH_INFO'] == '/' or environ['PATH_INFO'] == "":
        data = open(os.path.join(
                     os.path.dirname(__file__), 
                     'websocket.html')).read()
        data = data % environ
        start_response('200 OK', [('Content-Type', 'text/html'),
                                 ('Content-Length', len(data))])
        return [data]
    else:
        return wsapp(environ, start_response)


def server(**kwargs):
    wsgi = pulsar.require('wsgi')
    return wsgi.createServer(callable = app, **kwargs)


def start_server(**params):
    return server(**params).start()

    
if __name__ == '__main__':
    start_server()
