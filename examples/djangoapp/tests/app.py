'''Tests django chat application.'''
import json

from pulsar import send, get_application, Deferred
from pulsar.utils.path import Path
from pulsar.apps import wsgi, http, ws
from pulsar.apps.test import unittest, dont_run_with_thread

try:
    manage = Path(__file__).add2python('manage', up=1)
except ImportError:
    manage = None


def start_server(actor, name, argv):
    # we need to make sure djangoapp is in the python path
    actor.params.django_pulsar_name = name
    manage.execute_from_command_line(argv)
    app = yield get_application(name)
    yield app.event('start')


class MessageHandler(ws.WS):
    
    def __init__(self):
        self.new_future()
        
    def new_future(self):
        self.future = Deferred()
        
    def on_message(self, request, message):
        future = self.future
        self.new_future()
        future.callback(message)


@unittest.skipUnless(manage, 'Requires django')
class TestDjangoChat(unittest.TestCase):
    concurrency = 'thread'
    app = None
    
    @classmethod
    def setUpClass(cls):
        name = 'django_' + cls.concurrency
        argv = [__file__, 'pulse', '--bind', '127.0.0.1:0',
                '--concurrency', cls.concurrency]
        cls.app = yield send('arbiter', 'run', start_server, name, argv)
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.ws = 'ws://{0}:{1}/message'.format(*cls.app.address)
        cls.http = http.HttpClient()
        
    @classmethod
    def tearDownClass(cls):
        if cls.app:
            return send('arbiter', 'kill_actor', cls.app.name)

    def test_home(self):
        result = yield self.http.get(self.uri).on_finished
        self.assertEqual(result.status_code, 200)
        
    def test_404(self):
        result = yield self.http.get('%s/bsjdhcbjsdh' % self.uri).on_finished
        self.assertEqual(result.status_code, 404)
        
    def test_handshake(self):
        ws = yield self.http.get(self.ws).on_finished
        response = ws.handshake 
        self.assertEqual(response.status_code, 101)
        self.assertEqual(response.headers['upgrade'], 'websocket')
        self.assertEqual(response.connection, None)
        self.assertTrue(ws.connection)
        
    def test_websocket(self):
        ws = yield self.http.get(self.ws).on_finished
        self.assertTrue(ws)
        ws.handler = MessageHandler()
        ws.write('Hello there!')
        data = yield ws.handler.future
        data = json.loads(data)
        self.assertEqual(data['message'], 'Hello there!')
        
        
@dont_run_with_thread
class TestDjangoChat_Process(TestDjangoChat):
    concurrency = 'process'
    