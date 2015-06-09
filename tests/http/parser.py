import os
import unittest

from pulsar.utils.httpurl import hasextensions
from pulsar.utils import httpurl


class TestPythonHttpParser(unittest.TestCase):

    @classmethod
    def parser(cls, **kwargs):
        return httpurl.HttpParser(**kwargs)

    def __test_amazon_protocol_error(self):
        all = []
        for n in range(18):
            with open(os.path.join(os.path.dirname(__file__), 'data',
                                   'data%d.dat' % n), 'rb') as f:
                all.append(f.read())
        #
        p = self.parser()
        for chunk in all:
            self.assertEqual(p.execute(chunk, len(chunk)), len(chunk))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())

    def test_client_200_OK_no_headers(self):
        p = self.parser()
        data = b'HTTP/1.1 200 OK\r\n\r\n'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertFalse(p.is_partial_body())
        self.assertFalse(p.is_message_complete())

    def test_simple_server_message(self):
        p = self.parser()
        self.assertFalse(p.is_headers_complete())
        data = b'GET /forum/bla?page=1#post1 HTTP/1.1\r\n\r\n'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertEqual(p.get_fragment(), 'post1')
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        self.assertFalse(p.get_headers())

    def test_bad_combinations(self):
        p = self.parser()
        # body with no headers not valid
        data = b'GET /get HTTP/1.1\r\nciao'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertFalse(p.is_headers_complete())
        # self.assertTrue(p.is_message_begin())
        self.assertFalse(p.is_message_complete())
        self.assertEqual(p.execute(b'x', 1), 1)

    def test_client_connect(self):
        p = self.parser()
        data = b'HTTP/1.1 200 Connection established\r\n\r\n'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertFalse(p.is_partial_body())
        self.assertFalse(p.is_message_complete())

    def test_client_no_body(self):
        p = self.parser()
        data = (b'HTTP/1.1 200 OK\r\n'
                b'Connection: keep-alive\r\n\r\n')
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertFalse(p.is_partial_body())
        self.assertFalse(p.is_message_complete())

    def testBadFirstLine(self):
        p = self.parser()
        self.assertFalse(p.is_headers_complete())
        data = b'GET HTTP/1.1\r\n\r\n'
        self.assertNotEqual(p.execute(data, len(data)), len(data))
        p = self.parser()
        data = b'get /foo HTTP/1.1\r\n\r\n'
        self.assertNotEqual(p.execute(data, len(data)), len(data))
        p = self.parser()
        data = b'GET /foo FTP/1.1\r\n\r\n'
        self.assertNotEqual(p.execute(data, len(data)), len(data))
        p = self.parser()
        data = b'GET get/ HTTP/1-x\r\n'
        self.assertNotEqual(p.execute(data, len(data)), len(data))

    def testPartialFirstLine(self):
        p = self.parser()
        data = b'GET /get H'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertFalse(p.is_headers_complete())
        data = b'TTP/1.1\r\n\r\n'
        p.execute(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        self.assertFalse(p.get_headers())

    def testBadHeader(self):
        p = self.parser()
        data = b'GET /get HTTP/1.1\r\nbla\0: bar\r\n\r\n'
        self.assertNotEqual(p.execute(data, len(data)), len(data))
        #
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\nfoo\r\n\r\n'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_message_complete())
        headers = p.get_headers()
        self.assertEqual(len(headers), 0)

    def testHeaderOnly(self):
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\nHost: 0.0.0.0=5000\r\n\r\n'
        p.execute(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_message_complete())

    def testContentLength(self):
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\n'
        p.execute(data, len(data))
        self.assertFalse(p.is_headers_complete())
        data = b'Content-Length: 4\r\n\r\n'
        p.execute(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertFalse(p.is_message_complete())
        data = b'cia'
        p.execute(data, len(data))
        self.assertFalse(p.is_message_complete())
        data = b'o'
        p.execute(data, len(data))
        self.assertTrue(p.is_message_complete())

    def test_ouble_header(self):
        p = self.parser()
        data = b'GET /test HTTP/1.1\r\n'
        p.execute(data, len(data))
        data = b'Accept: */*\r\n'
        p.execute(data, len(data))
        data = b'Accept: jpeg\r\n\r\n'
        p.execute(data, len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_message_complete())
        headers = p.get_headers()
        self.assertEqual(len(headers), 1)
        self.assertEqual(headers.get('Accept'), ['*/*', 'jpeg'])

    def test_connect(self):
        p = self.parser()
        data = b'HTTP/1.1 200 Connection established\r\n\r\n'
        self.assertEqual(p.execute(data, len(data)), len(data))


@unittest.skipUnless(hasextensions, 'Requires C extensions')
class TestCHttpParser(TestPythonHttpParser):

    @classmethod
    def parser(cls, **kwargs):
        return httpurl.CHttpParser(**kwargs)
