'''Tests the "helloworld" example.'''
import unittest

from pulsar import send, SERVER_SOFTWARE, get_application, get_actor
from pulsar.apps.http import HttpClient
from pulsar.apps.test import run_on_arbiter, dont_run_with_thread

from .manage import server


class TestHelloWorldThread(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    def name(cls):
        return 'helloworld_' + cls.concurrency

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.name(), concurrency=cls.concurrency,
                   bind='127.0.0.1:0')
        cls.app_cfg = yield send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app_cfg.addresses[0])
        cls.client = HttpClient()

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            yield send('arbiter', 'kill_actor', cls.app_cfg.name)

    @run_on_arbiter
    def testMeta(self):
        app = yield get_application(self.name())
        self.assertEqual(app.name, self.name())
        monitor = get_actor().get_actor(app.name)
        self.assertTrue(monitor.is_running())
        self.assertEqual(app, monitor.app)
        self.assertEqual(str(app), app.name)
        self.assertEqual(app.cfg.bind, '127.0.0.1:0')

    def testResponse(self):
        c = self.client
        response = yield c.get(self.uri)
        self.assertEqual(response.status_code, 200)
        content = response.get_content()
        self.assertEqual(content, b'Hello World!\n')
        headers = response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['content-type'], 'text/plain')
        self.assertEqual(headers['server'], SERVER_SOFTWARE)

    def testTimeIt(self):
        c = self.client
        b = yield c.timeit('get', 5, self.uri)
        self.assertTrue(b.taken >= 0)


@dont_run_with_thread
class TestHelloWorldProcess(TestHelloWorldThread):
    concurrency = 'process'
