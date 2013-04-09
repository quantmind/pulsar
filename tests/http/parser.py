from pulsar.lib import hasextensions
from pulsar.apps.test import unittest
from pulsar.utils import httpurl

class TestHttpParser(unittest.TestCase):
    
    def parser(self, **kwargs):
        return httpurl.HttpParser(**kwargs)
    
    def testSimple(self):
        p = self.parser()
        self.assertFalse(p.is_headers_complete())
        data = b'GET /forum/bla?page=1#post1 HTTP/1.1\r\n\r\n'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertEqual(p.get_fragment(), 'post1')
        self.assertTrue(p.is_message_begin())
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        self.assertFalse(p.get_headers())
        
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
        
    def testBadCombinations(self):
        p = self.parser()
        # body with no headers not valid
        data = b'GET /get HTTP/1.1\r\nciao'
        self.assertEqual(p.execute(data, len(data)), len(data))
        self.assertNotEqual(p.execute(b'', 0), 0)
        p = self.parser()
        data = b'GET HTTP/1.1\r\n\r\n'
        self.assertNotEqual(p.execute(data, len(data)), len(data))
        
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
        
    def testDoubleHeader(self):
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
        self.assertEqual(headers.get('accept'), '*/*, jpeg')
        
        
@unittest.skipUnless(hasextensions, 'Requires C extensions')
class TestCHttpParser(TestHttpParser):
    
    def parser(self, **kwargs):
        from pulsar.lib import HttpParser
        return HttpParser(**kwargs)
        