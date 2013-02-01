'''tests the httpurl stand-alone script.'''
import os
import time

from pulsar.apps.test import unittest
from pulsar.utils.httpurl import urlencode, Headers, accept_content_type,\
                                 WWWAuthenticate, hexmd5, CacheControl,\
                                 urlquote, unquote_unreserved, requote_uri,\
                                 remove_double_slash, appendslash, capfirst,\
                                 encode_multipart_formdata, http_date,\
                                 cookiejar_from_dict
from pulsar.utils.pep import to_bytes, native_str, force_native_str
from pulsar.apps.http import Auth, HTTPBasicAuth, HTTPDigestAuth,\
                                parse_authorization_header, basic_auth_str


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
        
    def test_accept_content_type(self):
        accept = accept_content_type()
        self.assertTrue('text/html' in accept)
        accept = accept_content_type(
                        'text/*, text/html, text/html;level=1, */*')
        self.assertTrue('text/html' in accept)
        self.assertTrue('text/plain' in accept)
        
        
class TestAuth(unittest.TestCase):
    
    def testBase(self):
        auth = Auth()
        self.assertRaises(NotImplementedError, auth, None)
        self.assertFalse(auth.authenticated())
        self.assertEqual(str(auth), repr(auth))
        auth = HTTPBasicAuth('bla', 'foo')
        self.assertEqual(str(auth), 'Basic: bla')
        
    def test_WWWAuthenticate_basic(self):
        auth = WWWAuthenticate.basic('authenticate please')
        self.assertEqual(auth.type, 'basic')
        self.assertEqual(len(auth.options), 1)
        self.assertEqual(str(auth), 'Basic realm="authenticate please"')
        
    def test_WWWAuthenticate_digest(self):
        H = hexmd5
        nonce = H(to_bytes('%d' % time.time()) + os.urandom(10))
        auth = WWWAuthenticate.digest('www.mydomain.org', nonce,
                                    opaque=H(os.urandom(10)),
                                    qop=('auth', 'auth-int'))
        self.assertEqual(auth.options['qop'], 'auth, auth-int')
        
    def testDigest(self):
        auth = HTTPDigestAuth('bla', options={'realm': 'fake realm'})
        self.assertEqual(auth.type, 'digest')
        self.assertEqual(auth.username, 'bla')
        self.assertEqual(auth.password, None)
        self.assertEqual(auth.options['realm'], 'fake realm')
        
    def test_parse_authorization_header(self):
        parse = parse_authorization_header
        self.assertEqual(parse(''), None)
        self.assertEqual(parse('csdcds'), None)
        self.assertEqual(parse('csdcds cbsdjchbjsc'), None)
        self.assertEqual(parse('basic cbsdjcbsjchbsd'), None)
        auths = basic_auth_str('pippo', 'pluto')
        self.assertTrue(parse(auths).authenticated({}, 'pippo', 'pluto'))
        
    def test_CacheControl(self):
        headers = Headers()
        c = CacheControl()
        self.assertFalse(c.private)
        self.assertFalse(c.maxage)
        c(headers)
        self.assertEqual(headers['cache-control'], 'no-cache')
        c = CacheControl(maxage=3600)
        c(headers)
        self.assertEqual(headers['cache-control'], 'max-age=3600, public')
        c = CacheControl(maxage=3600, private=True)
        c(headers)
        self.assertEqual(headers['cache-control'], 'max-age=3600, private')
        c = CacheControl(maxage=3600, must_revalidate=True)
        c(headers)
        self.assertEqual(headers['cache-control'],
                            'max-age=3600, public, must-revalidate')
        c = CacheControl(maxage=3600, proxy_revalidate=True)
        c(headers)
        self.assertEqual(headers['cache-control'],
                            'max-age=3600, public, proxy-revalidate')
        c = CacheControl(maxage=3600, proxy_revalidate=True,
                                 nostore=True)
        c(headers)
        self.assertEqual(headers['cache-control'], 'no-store')
    
    
class TestTools(unittest.TestCase):
    
    def test_to_bytes(self):
        s = to_bytes('ciao')
        self.assertTrue(isinstance(s, bytes))
        s2 = to_bytes(s)
        self.assertEqual(id(s), id(s2))
        s3 = to_bytes(s, 'latin-1')
        self.assertEqual(s, s3)
        self.assertNotEqual(id(s), id(s3))
        
    def test_native_str(self):
        s = 'ciao'
        s2 = native_str(s)
        self.assertEqual(id(s), id(s2))
        
    def test_force_native_str(self):
        self.assertEqual(force_native_str('ciao'), 'ciao')
        self.assertEqual(force_native_str(b'ciao'), 'ciao')
        self.assertEqual(force_native_str(1), '1')
        self.assertEqual(force_native_str((1, 'b')), str((1, 'b')))
        
    def test_quote_unreserved(self):
        '''Test a string of unreserved characters'''
        s = 'a~b_(c-d).'
        qs = urlquote(s)
        self.assertTrue('%' in qs)
        uqs = unquote_unreserved(qs)
        self.assertEqual(uqs, s)
        self.assertEqual(requote_uri(s), s)
        self.assertEqual(requote_uri(qs), s)
        
    def test_quote_unreserved_percent(self):
        s = 'a=3.5%;b=2%'
        qs = urlquote(s)
        self.assertTrue('%' in qs)
        uqs = unquote_unreserved(qs)
        self.assertNotEqual(uqs, s)
        s = 'a~b_(c-d).'
        qs = urlquote(s) + '%f'
        uqs = unquote_unreserved(qs)
        self.assertEqual(uqs, s+'%f')

    def test_remove_double_slash(self):
        r = remove_double_slash
        self.assertEqual(r('/bla//foo/'), '/bla/foo/')
        self.assertEqual(r('/bla/////////foo//////////'), '/bla/foo/')
        self.assertEqual(r('/bla/foo/'), '/bla/foo/')
        
    def test_appendslash(self):
        self.assertEqual(appendslash('bla'), 'bla/')
        self.assertEqual(appendslash('bla/'), 'bla/')
        
    def test_capfirst(self):
        c = capfirst
        self.assertEqual(c('blA'), 'Bla')
        self.assertEqual(c(''), '')
        self.assertEqual(c('bOlA'), 'Bola')
        
    def test_encode_multipart_formdata(self):
        data, ct = encode_multipart_formdata([('bla', 'foo'),
                                                ('foo', ('pippo', 'pluto'))])
        idx = data.find(b'\r\n')
        boundary = data[2:idx].decode('utf-8')
        self.assertEqual(ct, 'multipart/form-data; boundary=%s' % boundary)
        
    def test_http_date(self):
        now = time.time()
        fmt = http_date(now)
        self.assertTrue(fmt.endswith(' GMT'))
        self.assertEqual(fmt[3:5], ', ')
        
    def test_cookiejar_from_dict(self):
        j = cookiejar_from_dict({'bla': 'foo'})
        j2 = cookiejar_from_dict({'pippo': 'pluto'}, j)
        self.assertEqual(j, j2)
        
