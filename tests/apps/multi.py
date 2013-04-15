'''Tests the rpc middleware and utilities. It uses the calculator example.'''
from pulsar import Config
from pulsar.apps import MultiApp
from pulsar.apps.wsgi import WSGIServer
from pulsar.apps.test import unittest


def dummy(environ, start_response):
    start_response('200 OK', [])
    yield [b'dummy']
    
class MultiWsgi(MultiApp):
    cfg = Config(bind=':0', rpc_bind=':0')
    
    def build(self):
        yield self.new_app(WSGIServer, callbale=dummy)
        yield self.new_app(WSGIServer, 'rpc', callbale=dummy)
    

class TestMultiApp(unittest.TestCase):
    
    def create(self, **params):
        return MultiWsgi(**params)
    
    def testApp(self):
        app = self.create()
        self.assertTrue(app)
        apps = app.apps()
        self.assertEqual(len(apps), 2)
        
    def testBacklog(self):
        app = self.create(backlog=20)
        self.assertEqual(app.cfg.backlog, 20)
        apps = app.apps()
        app1 = apps[0]
        self.assertEqual(app1.cfg.backlog, 20)
        
        