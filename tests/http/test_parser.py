import unittest

from multidict import CIMultiDict

from pulsar.utils.http import (
    HttpRequestParser, HttpResponseParser, HttpParserError
)


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
        p.parser.feed_data(b'HTTP/1.1 200 OK\r\n\r\n')
        self.assertTrue(p.headers_complete)
        self.assertFalse(p.message_complete)

    def test_client_500(self):
        p = self.response()
        p.parser.feed_data(b'HTTP/1.1 500 Internal Server Error\r\n\r\n')
        self.assertTrue(p.headers_complete)
        self.assertFalse(p.message_complete)

    def test_expect(self):
        p = self.response()
        data = b'HTTP/1.1 100 Continue\r\n\r\n'
        p.feed_data(data)
        self.assertTrue(p.headers_complete)
        self.assertTrue(p.message_complete)

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
        p = self.request()
        data = b'GET HTTP/1.1\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)
        p = self.request()
        data = b'get /foo HTTP/1.1\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)
        p = self.request()
        data = b'GET /foo FTP/1.1\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)
        p = self.request()
        data = b'GET get/ HTTP/1-x\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)

    def testPartialFirstLine(self):
        p = self.request()
        p.parser.feed_data(b'GET /get H')
        self.assertFalse(p.headers_complete)
        p.parser.feed_data(b'TTP/1.1\r\n')
        self.assertEqual(p.url, b'/get')
        self.assertFalse(p.headers_complete)
        p.parser.feed_data(b'\r\n')
        self.assertTrue(p.headers_complete)
        self.assertTrue(p.message_complete)
        self.assertFalse(p.headers)

    def testBadHeader(self):
        #
        p = self.request()
        data = b'GET /test HTTP/1.1\r\nfoo\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)
        #
        p = self.request()
        data = b'GET /get HTTP/1.1\r\nbla\7: bar\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)
        #
        p = self.request()
        data = b'GET /get HTTP/1.1\r\nbla\42: bar\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)
        #
        p = self.request()
        data = b'GET /get HTTP/1.1\r\nbla\128: bar\r\n\r\n'
        self.assertRaises(HttpParserError, p.parser.feed_data, data)

    def testHeaderOnly(self):
        p = self.request()
        data = b'GET /test HTTP/1.1\r\nHost: 0.0.0.0=5000\r\n\r\n'
        p.parser.feed_data(data)
        self.assertTrue(p.headers_complete)
        self.assertTrue(p.message_complete)

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
        p = self.request()
        p.parser.feed_data(b'GET /test HTTP/1.1\r\n')
        self.assertFalse(p.headers_complete)
        p.parser.feed_data(b'Accept: */*\r\n')
        self.assertFalse(p.headers_complete)
        p.parser.feed_data(b'Accept: jpeg\r\n\r\n')
        self.assertTrue(p.headers_complete)
        self.assertEqual(len(p.headers), 2)
        self.assertEqual(p.headers.getall('Accept'), ['*/*', 'jpeg'])

    def test_connection_established(self):
        p = self.response()
        p.parser.feed_data((
            b'HTTP/1.1 200 Connection established\r\n'
            b'Server: Pulsar-proxy-server/2.0.0a1\r\n'
            b'Date: Thu, 16 Nov 2017 11:41:51 GMT\r\n\r\n'
        ))
        self.assertTrue(p.headers_complete)
        self.assertFalse(p.message_complete)
