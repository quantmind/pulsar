'''Tests the "flaskapp" example.'''
import unittest

from pulsar import SERVER_SOFTWARE
from pulsar.api import send
from pulsar.apps.http import HttpClient
from pulsar.apps.test import run_test_server

try:
    from examples.flaskapp.manage import server
except ImportError:
    server = None


@unittest.skipUnless(server, "Requires flask module")
class TestFlaskApp(unittest.TestCase):
    app_cfg = None

    @classmethod
    async def setUpClass(cls):
        await run_test_server(cls, server)
        cls.client = HttpClient()

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    async def test_response_200(self):
        c = self.client
        response = await c.get(self.uri)
        self.assertEqual(response.status_code, 200)
        content = response.content
        self.assertEqual(content, b'Flask Example')
        headers = response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['server'], SERVER_SOFTWARE)

    async def test_response_404(self):
        c = self.client
        response = await c.get('%s/bh' % self.uri)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.content, b'404 Page')
