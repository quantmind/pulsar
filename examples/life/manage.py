'''
To run the example, simply type::

    pulsar manage.py
    
'''
import json
import os
import time
from collections import Counter

try:
    import pulsar
except ImportError:
    import sys
    sys.path.append('../../')
    import pulsar
from pulsar import command, async
from pulsar.apps import wsgi, ws


class GameOfLife:
    def __init__(self, initial, scale=1):
        s = 1 / scale
        self.universe = set(((int(v[0]*s), int(v[1]*s)) for v in initial))
    
    def check(self, x, y, universe, neighbours):
        if x < 0 or y < 0:
            return
        xy = (x, y)
        if xy in universe:
            neighbours[xy] += 1
            
    def next_generation(self):
        neighbours = Counter()
        next = set()
        universe = self.universe
        check = self.check
        for x, y in universe:
            check(x+1, y-1, universe, neighbours)
            check(x+1, y,   universe, neighbours)
            check(x+1, y+1, universe, neighbours)
            check(x,   y+1, universe, neighbours)
            check(x-1, y+1, universe, neighbours)
            check(x-1, y,   universe, neighbours)
            check(x-1, y-1, universe, neighbours)
            check(x,   y-1, universe, neighbours)
        # Check current alive
        alive = []
        dead = []
        for xy in universe:
            n = neighbours.pop(xy, 0)
            if n > 1 and n < 4:
                next.append(xy)
            else:
                dead.append(list(xy))
        # Check remainings
        for xy, n in neighbours.items():
            if n == 3:
                next.append(xy)
                new_alive.append(list(xy))
        self.universe = next
        return alive, dead
        
    
################################################################################
##    WEB APPLICATION

class handle(ws.WS):
    
    def match(self, environ):
        return environ.get('PATH_INFO') == '/life'
    
    def get_data(self, frame):
        return json.loads(frame)
    
    def on_message(self, environ, frame):
        data = self.get_data(frame)
        action = data['action']
        if action == 'start':
            environ['life'] = life = GameOfLife(data['data'], data['scale'])
        elif action == 'next':
            life = environ['life']
        alive, dead = life.next_generation()
        return json.dumps((alive, dead))


def page(environ, start_response):
    path = environ.get('PATH_INFO')
    if not path or path == '/':
        data = open(os.path.join(os.path.dirname(__file__), 
                     'life.html')).read()
        data = data % environ
        start_response('200 OK', [('Content-Type', 'text/html'),
                                  ('Content-Length', str(len(data)))])
        return [pulsar.to_bytes(data)]


def server(**kwargs):
    app = wsgi.WsgiHandler(middleware=(page, ws.WebSocket(handle())))
    return wsgi.WSGIServer(callable=app, **kwargs)
    
    
if __name__ == '__main__':
    server().start()