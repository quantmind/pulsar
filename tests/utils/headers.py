from pulsar.apps.test import unittest
from pulsar.utils.httpurl import Headers, accept_content_type


class TestHeaders(unittest.TestCase):
    
    def testServerHeader(self):
        h = Headers()
        self.assertEqual(h.kind, 'server')
        self.assertEqual(len(h), 0)
        h['content-type'] = 'text/html'
        self.assertEqual(len(h), 1)
        
    def testHeaderBytes(self):
        h = Headers(kind=None)
        h['content-type'] = 'text/html'
        h['server'] = 'bla'
        self.assertTrue(repr(h).startswith('both '))
        self.assertEqual(bytes(h), b'Server: bla\r\n'
                                   b'Content-Type: text/html\r\n\r\n')
        
    def testClientHeader(self):
        h = Headers(kind='client')
        self.assertEqual(h.kind, 'client')
        self.assertEqual(len(h), 0)
        h['content-type'] = 'text/html'
        self.assertEqual(h.get_all('content-type'), ['text/html'])
        self.assertEqual(len(h), 1)
        h['server'] = 'bla'
        self.assertEqual(len(h), 1)
        del h['content-type']
        self.assertEqual(len(h), 0)
        self.assertEqual(h.get_all('content-type', []), [])
        
    def test_non_standard_request_headers(self):
        h = Headers(kind='client')
        h['accept'] = 'text/html'
        self.assertEqual(len(h), 1)
        h['server'] = 'bla'
        self.assertEqual(len(h), 1)
        h['proxy-connection'] = 'keep-alive'
        self.assertEqual(len(h), 2)
        headers = str(h)
        self.assertTrue('Proxy-Connection:' in headers)
    
    def test_multiple_entry(self):
        h = Headers([('Connection', 'Keep-Alive'),
                     ('Accept-Encoding', 'identity'),
                     ('Accept-Encoding', 'deflate'),
                     ('Accept-Encoding', 'compress'),
                     ('Accept-Encoding', 'gzip')],
                     kind='client')
        accept = h['accept-encoding']
        self.assertEqual(accept, 'identity, deflate, compress, gzip')
        
    def test_accept_content_type(self):
        accept = accept_content_type()
        self.assertTrue('text/html' in accept)
        accept = accept_content_type(
                        'text/*, text/html, text/html;level=1, */*')
        self.assertTrue('text/html' in accept)
        self.assertTrue('text/plain' in accept)