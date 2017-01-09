import unittest

from multidict import CIMultiDict

from pulsar.utils import http
from pulsar.utils.http.parser import HttpRequestParser, HttpResponseParser


class Protocol:

    def __init__(self, Parser, **kwargs):
        self.url = None
        self.headers = CIMultiDict()
        self.body = b''
        self.headers_complete = False
        self.message_complete = False
        self.parser = Parser(self, **kwargs)
        self.feed_data = self.parser.feed_data

    def on_url(self, url):
        self.url = url

    def on_header(self, name, value):
        self.headers.add(name.decode(), value.decode())

    def on_headers_complete(self):
        self.headers_complete = True

    def on_body(self, body):
        self.body += body

    def on_message_complete(self):
        self.message_complete = True


class TestPythonHttpParser(unittest.TestCase):

    @classmethod
    def request(cls, **kwargs):
        return Protocol(HttpRequestParser, **kwargs)

    @classmethod
    def response(cls, **kwargs):
        return Protocol(HttpResponseParser, **kwargs)

    def test_client_200_OK_no_headers(self):
        p = self.response()
        data = b'HTTP/1.1 200 OK\r\n\r\n'
        p.parser.feed_data(data)
        self.assertTrue(p.headers_complete)
        self.assertFalse(p.message_complete)

    def test_simple_server_message(self):
        p = self.request()
        data = b'GET /forum/bla?page=1#post1 HTTP/1.1\r\n\r\n'
        p.parser.feed_data(data)
        self.assertTrue(p.url)
        self.assertEqual(p.parser.get_http_version(), '1.1')
        self.assertTrue(p.headers_complete)
        self.assertTrue(p.message_complete)
        self.assertFalse(p.headers)

    def test_bad_combinations(self):
        p = self.request()
        # body with no headers not valid
        data = b'GET /get HTTP/1.1\r\nciao'
        p.parser.feed_data(data)
        self.assertFalse(p.headers_complete)
        self.assertFalse(p.message_complete)

    def test_client_connect(self):
        p = self.response()
        data = b'HTTP/1.1 200 Connection established\r\n\r\n'
        p.feed_data(data)
        self.assertTrue(p.headers_complete)
        self.assertEqual(p.parser.get_status_code(), 200)
        self.assertFalse(p.message_complete)

    def test_client_no_body(self):
        p = self.response()
        data = (b'HTTP/1.1 200 OK\r\n'
                b'Connection: keep-alive\r\n\r\n')
        p.feed_data(data)
        self.assertTrue(p.headers_complete)
        self.assertFalse(p.message_complete)
        self.assertEqual(len(p.headers), 1)
        self.assertEqual(p.headers['connection'], 'keep-alive')

    def testBadFirstLine(self):
        p = self.parser()
        self.assertFalse(p.is_headers_complete())
        data = b'GET HTTP/1.1\r\n\r\n'
        self.assertNotEqual(p.feed_data(data, len(data)), len(data))
        p = self.parser()
        data = b'get /foo HTTP/1.1\r\n\r\n'
        self.assertNotEqual(p.feed_data(data, len(data)), len(data))
        p = self.parser()
        data = b'GET /foo FTP/1.1\r\n\r\n'
        self.assertNotEqual(p.feed_data(data, len(data)), len(data))
        p = self.parser()
        data = b'GET get/ HTTP/1-x\r\n'
        self.assertNotEqual(p.feed_data(data, len(data)), len(data))

    def testPartialFirstLine(self):
        p = self.parser()
        data = b'GET /get H'
        self.assertEqual(p.feed_data(data, len(data)), len(data))
        self.assertFalse(p.is_headers_complete())
        data = b'TTP/1.1\r\n\r\n'
        p.feed_data(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        self.assertFalse(p.get_headers())

    def testBadHeader(self):
        p = self.parser()
        data = b'GET /get HTTP/1.1\r\nbla\0: bar\r\n\r\n'
        self.assertNotEqual(p.feed_data(data, len(data)), len(data))
        #
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\nfoo\r\n\r\n'
        self.assertEqual(p.feed_data(data, len(data)), len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_message_complete())
        headers = p.get_headers()
        self.assertEqual(len(headers), 0)

    def testHeaderOnly(self):
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\nHost: 0.0.0.0=5000\r\n\r\n'
        p.feed_data(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_message_complete())

    def test_server_content_length(self):
        p = self.request()
        data = b'GET /test HTTP/1.1\r\n'
        p.feed_data(data)
        self.assertFalse(p.headers_complete)
        data = b'Content-Length: 4\r\n\r\n'
        p.feed_data(data)
        self.assertTrue(p.headers_complete)
        self.assertFalse(p.message_complete)
        data = b'cia'
        p.feed_data(data)
        self.assertFalse(p.message_complete)
        data = b'o'
        p.feed_data(data)
        self.assertTrue(p.message_complete)
        self.assertEqual(p.body, b'ciao')

    def test_double_header(self):
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\n'
        p.feed_data(data, len(data))
        data = b'Accept: */*\r\n'
        p.feed_data(data)
        data = b'Accept: jpeg\r\n\r\n'
        p.feed_data(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_message_complete())
        headers = p.get_headers()
        self.assertEqual(len(headers), 1)
        self.assertEqual(headers.get('Accept'), '*/*, jpeg')

    def test_connect(self):
        p = self.parser()
        data = b'HTTP/1.1 200 Connection established\r\n\r\n'
        p.feed_data(data)


@unittest.skipUnless(http.hasextensions, 'Requires C extensions')
class TestCHttpParser(TestPythonHttpParser):

    @classmethod
    def request(cls, **kwargs):
        return Protocol(http.HttpRequestParser, **kwargs)

    @classmethod
    def response(cls, **kwargs):
        return Protocol(http.HttpResponseParser, **kwargs)
