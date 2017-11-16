import unittest

from pulsar.apps.wsgi import WsgiResponse
from pulsar.utils.lib import WsgiResponse as Wsgi


common = {
    200: 'OK',
    400: 'Bad Request',
    404: 'Not Found',
    500: 'Internal Server Error'
}


class TestWsgi(unittest.TestCase):
    __benchmark__ = True
    __number__ = 20000

    def setUp(self):
        self.environ = {}

    def test_python(self):
        WsgiResponse(environ=self.environ, status_code=200)

    def test_cython(self):
        Wsgi(environ=self.environ, status_code=200)

    def test_python_content(self):
        WsgiResponse(environ=self.environ, status_code=200,
                     content='this is a test')

    def test_cython_content(self):
        Wsgi(environ=self.environ, status_code=200, content='this is a test')

    def test_python_headers(self):
        r = WsgiResponse(environ=self.environ, status_code=200,
                         content='this is a test')
        r.get_headers()

    def test_cython_headers(self):
        r = Wsgi(environ=self.environ, status_code=200,
                 content='this is a test')
        r.get_headers()
