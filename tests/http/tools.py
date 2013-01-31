'''tests the httpurl stand-alone script.'''
import os
import time

from pulsar import send, make_async, safe_async, is_failure
from pulsar.apps.test import unittest
from pulsar.utils import httpurl
from pulsar.utils.httpurl import to_bytes, urlencode

BIN_HOST = 'httpbin.org'
HTTPBIN_URL = 'http://' + BIN_HOST + '/'
HTTPSBIN_URL = 'https://'+ BIN_HOST + '/'


class TestHeaders(unittest.TestCase):
    
    def testServerHeader(self):
        h = httpurl.Headers()
        self.assertEqual(h.kind, 'server')
        self.assertEqual(len(h), 0)
        h['content-type'] = 'text/html'
        self.assertEqual(len(h), 1)
        
    def testHeaderBytes(self):
        h = httpurl.Headers(kind=None)
        h['content-type'] = 'text/html'
        h['server'] = 'bla'
        self.assertTrue(repr(h).startswith('both '))
        self.assertEqual(bytes(h), b'Server: bla\r\n'
                                   b'Content-Type: text/html\r\n\r\n')
        
    def testClientHeader(self):
        h = httpurl.Headers(kind='client')
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
        accept = httpurl.accept_content_type()
        self.assertTrue('text/html' in accept)
        accept = httpurl.accept_content_type(
                        'text/*, text/html, text/html;level=1, */*')
        self.assertTrue('text/html' in accept)
        self.assertTrue('text/plain' in accept)
        
        
class TestAuth(unittest.TestCase):
    
    def testBase(self):
        auth = httpurl.Auth()
        self.assertRaises(NotImplementedError, auth, None)
        self.assertFalse(auth.authenticated())
        self.assertEqual(str(auth), repr(auth))
        auth = httpurl.HTTPBasicAuth('bla', 'foo')
        self.assertEqual(str(auth), 'Basic: bla')
        
    def test_WWWAuthenticate_basic(self):
        auth = httpurl.WWWAuthenticate.basic('authenticate please')
        self.assertEqual(auth.type, 'basic')
        self.assertEqual(len(auth.options), 1)
        self.assertEqual(str(auth), 'Basic realm="authenticate please"')
        
    def test_WWWAuthenticate_digest(self):
        H = httpurl.hexmd5
        nonce = H(to_bytes('%d' % time.time()) + os.urandom(10))
        auth = httpurl.WWWAuthenticate.digest('www.mydomain.org', nonce,
                                    opaque=H(os.urandom(10)),
                                    qop=('auth', 'auth-int'))
        self.assertEqual(auth.options['qop'], 'auth, auth-int')
        
    def testDigest(self):
        auth = httpurl.HTTPDigestAuth('bla', options={'realm': 'fake realm'})
        self.assertEqual(auth.type, 'digest')
        self.assertEqual(auth.username, 'bla')
        self.assertEqual(auth.password, None)
        self.assertEqual(auth.options['realm'], 'fake realm')
        
    def test_parse_authorization_header(self):
        parse = httpurl.parse_authorization_header
        self.assertEqual(parse(''), None)
        self.assertEqual(parse('csdcds'), None)
        self.assertEqual(parse('csdcds cbsdjchbjsc'), None)
        self.assertEqual(parse('basic cbsdjcbsjchbsd'), None)
        auths = httpurl.basic_auth_str('pippo', 'pluto')
        self.assertTrue(parse(auths).authenticated({}, 'pippo', 'pluto'))
        
    def test_CacheControl(self):
        headers = httpurl.Headers()
        c = httpurl.CacheControl()
        self.assertFalse(c.private)
        self.assertFalse(c.maxage)
        c(headers)
        self.assertEqual(headers['cache-control'], 'no-cache')
        c = httpurl.CacheControl(maxage=3600)
        c(headers)
        self.assertEqual(headers['cache-control'], 'max-age=3600, public')
        c = httpurl.CacheControl(maxage=3600, private=True)
        c(headers)
        self.assertEqual(headers['cache-control'], 'max-age=3600, private')
        c = httpurl.CacheControl(maxage=3600, must_revalidate=True)
        c(headers)
        self.assertEqual(headers['cache-control'],
                            'max-age=3600, public, must-revalidate')
        c = httpurl.CacheControl(maxage=3600, proxy_revalidate=True)
        c(headers)
        self.assertEqual(headers['cache-control'],
                            'max-age=3600, public, proxy-revalidate')
        c = httpurl.CacheControl(maxage=3600, proxy_revalidate=True,
                                 nostore=True)
        c(headers)
        self.assertEqual(headers['cache-control'], 'no-store')
    
    
class TestTools(unittest.TestCase):
    
    def test_to_bytes(self):
        s = httpurl.to_bytes('ciao')
        self.assertTrue(isinstance(s, bytes))
        s2 = httpurl.to_bytes(s)
        self.assertEqual(id(s), id(s2))
        s3 = httpurl.to_bytes(s, 'latin-1')
        self.assertEqual(s, s3)
        self.assertNotEqual(id(s), id(s3))
        
    def test_native_str(self):
        s = 'ciao'
        s2 = httpurl.native_str(s)
        self.assertEqual(id(s), id(s2))
        
    def test_force_native_str(self):
        self.assertEqual(httpurl.force_native_str('ciao'), 'ciao')
        self.assertEqual(httpurl.force_native_str(b'ciao'), 'ciao')
        self.assertEqual(httpurl.force_native_str(1), '1')
        self.assertEqual(httpurl.force_native_str((1, 'b')), str((1, 'b')))
        
    def test_quote_unreserved(self):
        '''Test a string of unreserved characters'''
        s = 'a~b_(c-d).'
        qs = httpurl.urlquote(s)
        self.assertTrue('%' in qs)
        uqs = httpurl.unquote_unreserved(qs)
        self.assertEqual(uqs, s)
        self.assertEqual(httpurl.requote_uri(s), s)
        self.assertEqual(httpurl.requote_uri(qs), s)
        
    def test_quote_unreserved_percent(self):
        s = 'a=3.5%;b=2%'
        qs = httpurl.urlquote(s)
        self.assertTrue('%' in qs)
        uqs = httpurl.unquote_unreserved(qs)
        self.assertNotEqual(uqs, s)
        s = 'a~b_(c-d).'
        qs = httpurl.urlquote(s) + '%f'
        uqs = httpurl.unquote_unreserved(qs)
        self.assertEqual(uqs, s+'%f')

    def test_remove_double_slash(self):
        r = httpurl.remove_double_slash
        self.assertEqual(r('/bla//foo/'), '/bla/foo/')
        self.assertEqual(r('/bla/////////foo//////////'), '/bla/foo/')
        self.assertEqual(r('/bla/foo/'), '/bla/foo/')
        
    def test_appendslash(self):
        self.assertEqual(httpurl.appendslash('bla'), 'bla/')
        self.assertEqual(httpurl.appendslash('bla/'), 'bla/')
        
    def test_capfirst(self):
        c = httpurl.capfirst
        self.assertEqual(c('blA'), 'Bla')
        self.assertEqual(c(''), '')
        self.assertEqual(c('bOlA'), 'Bola')
        
    def test_encode_multipart_formdata(self):
        data, ct = httpurl.encode_multipart_formdata([('bla', 'foo'),
                                                ('foo', ('pippo', 'pluto'))])
        idx = data.find(b'\r\n')
        boundary = data[2:idx].decode('utf-8')
        self.assertEqual(ct, 'multipart/form-data; boundary=%s' % boundary)
        
    def test_HttpResponse(self):
        r = httpurl.HttpResponse(None)
        self.assertEqual(r.status_code, None)
        self.assertEqual(str(r), '<None>')
        
    def test_IOClientRead(self):
        io = httpurl.IOClientRead()
        self.assertEqual(io.read(), None)
        self.assertRaises(NotImplementedError, io.parsedata, b'bla')
        
    def test_http_date(self):
        now = time.time()
        fmt = httpurl.http_date(now)
        self.assertTrue(fmt.endswith(' GMT'))
        self.assertEqual(fmt[3:5], ', ')
        
    def test_cookiejar_from_dict(self):
        j = httpurl.cookiejar_from_dict({'bla': 'foo'})
        j2 = httpurl.cookiejar_from_dict({'pippo': 'pluto'}, j)
        self.assertEqual(j, j2)
        

    
class TestHttpClient:
        
    def test_Cookie(self):
        http = self.client()
        # First set the cookies
        r = make_async(http.get(self.httpbin('cookies', 'set', 'bla', 'foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.history)
        self.assertTrue(r.history[0].headers['set-cookie'])
        # Now check if I get them
        r = make_async(http.get(self.httpbin('cookies')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        result = r.content_json()
        self.assertTrue(result['cookies'])
        self.assertEqual(result['cookies']['bla'],'foo')
        # Try without saving cookies
        http = self.client(store_cookies=False)
        r = make_async(http.get(self.httpbin('cookies', 'set', 'bla', 'foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.history)
        self.assertTrue(r.history[0].headers['set-cookie'])
        r = make_async(http.get(self.httpbin('cookies')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        result = r.content_json()
        self.assertFalse(result['cookies'])

    def test_parse_cookie(self):
        self.assertEqual(httpurl.parse_cookie('invalid key=true'),
                         {'key':'true'})
        self.assertEqual(httpurl.parse_cookie('invalid;key=true'),
                         {'key':'true'})
        
    def test_basic_authentication(self):
        http = self.client()
        r = make_async(http.get(self.httpbin('basic-auth/bla/foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 401)
        http.add_basic_authentication('bla', 'foo')
        r = make_async(http.get(self.httpbin('basic-auth/bla/foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        
    def test_digest_authentication(self):
        http = self.client()
        r = make_async(http.get(self.httpbin('digest-auth/auth/bla/foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 401)
        http.add_digest_authentication('bla', 'foo')
        r = make_async(http.get(self.httpbin('digest-auth/auth/bla/foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
    
    #### TO INCLUDE
    def __test_far_expiration(self):
        "Cookie will expire when an distant expiration time is provided"
        response = Response(self.environ())
        response.set_cookie('datetime', expires=datetime(2028, 1, 1, 4, 5, 6))
        datetime_cookie = response.cookies['datetime']
        self.assertEqual(datetime_cookie['expires'], 'Sat, 01-Jan-2028 04:05:06 GMT')

    def __test_max_age_expiration(self):
        "Cookie will expire if max_age is provided"
        response = Response(self.environ())
        response.set_cookie('max_age', max_age=10)
        max_age_cookie = response.cookies['max_age']
        self.assertEqual(max_age_cookie['max-age'], 10)
        self.assertEqual(max_age_cookie['expires'], http.cookie_date(time.time()+10))

    def __test_httponly_cookie(self):
        response = Response(self.environ())
        response.set_cookie('example', httponly=True)
        example_cookie = response.cookies['example']
        # A compat cookie may be in use -- check that it has worked
        # both as an output string, and using the cookie attributes
        self.assertTrue('; httponly' in str(example_cookie))
        self.assertTrue(example_cookie['httponly'])
        
    