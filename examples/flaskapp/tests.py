'''Tests the "helloworld" example.'''
import unittest

from pulsar import send, SERVER_SOFTWARE
from pulsar.apps.http import HttpClient
from pulsar.apps.test import dont_run_with_thread

from .manage import server


class TestFlaskThread(unittest.TestCase):
    app_cfg = None
    concurrency = 'thread'

    @classmethod
    def name(cls):
        return 'flask_' + cls.concurrency

    @classmethod
    def setUpClass(cls):
        s = server(name=cls.name(),
                   concurrency=cls.concurrency,
                   bind='127.0.0.1:0')
        cls.app_cfg = yield from send('arbiter', 'run', s)
        cls.uri = 'http://{0}:{1}'.format(*cls.app_cfg.addresses[0])
        cls.client = HttpClient()

    @classmethod
    def tearDownClass(cls):
        if cls.app_cfg is not None:
            return send('arbiter', 'kill_actor', cls.app_cfg.name)

    def testResponse200(self):
        c = self.client
        response = yield from c.get(self.uri)
        self.assertEqual(response.status_code, 200)
        content = response.get_content()
        self.assertEqual(content, b'Flask Example')
        headers = response.headers
        self.assertTrue(headers)
        self.assertEqual(headers['server'], SERVER_SOFTWARE)

    def testResponse404(self):
        c = self.client
        response = yield from c.get('%s/bh' % self.uri)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_content(), b'404 Page')


@dont_run_with_thread
class TestFlaskProcess(TestFlaskThread):
    concurrency = 'process'
