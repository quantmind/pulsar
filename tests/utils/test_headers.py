import unittest

from pulsar.utils.httpurl import Headers, SimpleCookie


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

    def test_init_int(self):
        h = Headers(kind=1)
        self.assertEqual(h.kind, 'server')
        self.assertEqual(h.kind_number, 1)
        h = Headers(kind=0)
        self.assertEqual(h.kind, 'client')
        self.assertEqual(h.kind_number, 0)
        h = Headers(kind=56)
        self.assertEqual(h.kind, 'both')
        self.assertEqual(h.kind_number, 2)

    def test_remove_header(self):
        h = Headers([('Content-type', 'text/html')])
        self.assertEqual(len(h), 1)
        self.assertEqual(h.remove_header('foo'), None)
        self.assertEqual(h.remove_header('content-length'), None)
        self.assertEqual(h.remove_header('content-type'), ['text/html'])
        self.assertEqual(len(h), 0)

    def test_remove_header_value(self):
        h = Headers([('Accept-encoding', 'gzip'),
                     ('Accept-encoding', 'deflate'),
                     ('Accept', '*/*')], kind=2)
        self.assertEqual(len(h), 2)
        self.assertEqual(h['accept-encoding'], 'gzip, deflate')
        self.assertEqual(h.remove_header('accept-encoding', 'x'), None)
        self.assertEqual(h['accept-encoding'], 'gzip, deflate')
        self.assertEqual(h.remove_header('accept-encoding', 'deflate'),
                         'deflate')
        self.assertEqual(len(h), 2)
        self.assertEqual(h['accept-encoding'], 'gzip')

    def test_override(self):
        h = Headers([('Accept-encoding', 'gzip'),
                     ('Accept-encoding', 'deflate'),
                     ('Accept', '*/*')], kind=2)
        h.override([('Accept-encoding', 'gzip2'),
                    ('Accept-encoding', 'deflate2'),
                    ('Accept', 'text/html'),
                    ('Accept', '*/*; q=0.8')])
        self.assertEqual(len(h), 2)
        self.assertEqual(h['accept-encoding'], 'gzip2, deflate2')
        self.assertEqual(h['accept'], 'text/html, */*; q=0.8')

    def test_cookies(self):
        h = Headers()
        cookies = SimpleCookie({'bla': 'foo', 'pippo': 'pluto'})
        self.assertEqual(len(cookies), 2)
        for c in cookies.values():
            v = c.OutputString()
            h.add_header('Set-Cookie', v)
        h = str(h)
        self.assertTrue(
            h in ('Set-Cookie: bla=foo\r\nSet-Cookie: pippo=pluto\r\n\r\n',
                  'Set-Cookie: pippo=pluto\r\nSet-Cookie: bla=foo\r\n\r\n'))
