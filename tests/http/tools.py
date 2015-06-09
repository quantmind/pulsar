'''tests the httpurl stand-alone script.'''
import time
import unittest

from pulsar.utils.httpurl import (Headers, CacheControl,
                                  urlquote, unquote_unreserved, requote_uri,
                                  remove_double_slash, appendslash, capfirst,
                                  encode_multipart_formdata, http_date,
                                  cookiejar_from_dict)
from pulsar.apps.http import Auth, HTTPBasicAuth, HTTPDigestAuth


class TestAuth(unittest.TestCase):

    def test_auth(self):
        auth = Auth()
        self.assertRaises(NotImplementedError, auth, None)
        self.assertEqual(str(auth), repr(auth))

    def test_basic_auth(self):
        auth = HTTPBasicAuth('bla', 'foo')
        self.assertEqual(str(auth), 'Basic: bla')

    def test_digest_auth(self):
        auth = HTTPDigestAuth('bla', options={'realm': 'fake realm'})
        self.assertEqual(auth.type, 'digest')
        self.assertEqual(auth.username, 'bla')
        self.assertEqual(auth.password, None)
        self.assertEqual(auth.options['realm'], 'fake realm')

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
        self.assertEqual(headers['cache-control'],
                         'no-store, no-cache, must-revalidate, max-age=0')


class TestTools(unittest.TestCase):

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
        j = cookiejar_from_dict({'bla': 'foo'}, None)
        self.assertEqual(len(j), 1)
        self.assertEqual(j, cookiejar_from_dict(None, j))
        j2 = cookiejar_from_dict({'pippo': 'pluto'}, None, j)
        self.assertEqual(len(j2), 2)
