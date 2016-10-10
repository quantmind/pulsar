'''Tests the "flaskgreen" example.'''
import unittest

from pulsar import send, SERVER_SOFTWARE
from pulsar.apps.http import HttpClient
from pulsar.apps.test import dont_run_with_thread

try:
    from examples.flaskgreen.manage import FlaskGreen, log_connection
except ImportError:
    FlaskGreen = None


@unittest.skipUnless(FlaskGreen, "Requires flask and greenlet module")
class TestFlaskGreenThread(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    def name(cls):
        return 'flaskgreen_' + cls.concurrency

    @classmethod
    async def setUpClass(cls):
        s = FlaskGreen(name=cls.name(),
                       concurrency=cls.concurrency,
                       bind='127.0.0.1:0',
                       echo_bind='127.0.0.1:0')
        cls.app_cfg = await send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app_cfg[0].addresses[0])
        cls.client = HttpClient()

    @classmethod
    async def tearDownClass(cls):
        if cls.app_cfg is not None:
            await send('arbiter', 'kill_actor', cls.app_cfg[0].name)
            await send('arbiter', 'kill_actor', cls.app_cfg[1].name)

    def test_apps(self):
        self.assertEqual(self.app_cfg[0].name, self.name())
        self.assertEqual(self.app_cfg[1].name, 'echo_%s' % self.name())
        self.assertEqual(self.app_cfg[1].connection_made, log_connection)

    async def testResponse200(self):
        c = self.client
        response = await c.get(self.uri)
        self.assertEqual(response.status_code, 200)
        content = response.content
        self.assertEqual(content, b'Try any other url for an echo')
        headers = response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['server'], SERVER_SOFTWARE)

    async def testResponseEcho(self):
        c = self.client
        response = await c.get('%s/ciao' % self.uri)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'Flask Url : ciao')


@dont_run_with_thread
class TestFlaskGreenProcess(TestFlaskGreenThread):
    concurrency = 'process'
