'''Tests the wsgi middleware in pulsar.apps.wsgi'''
import time
import pickle
import unittest
from unittest import mock
from datetime import datetime, timedelta
from urllib.parse import urlparse

import pulsar
from pulsar.apps import wsgi
from pulsar.apps import http
from pulsar.apps.wsgi.utils import cookie_date


class WsgiRequestTests(unittest.TestCase):

    def request(self, **kwargs):
        environ = wsgi.test_wsgi_environ(**kwargs)
        return wsgi.WsgiRequest(environ)

    def test_absolute_path(self):
        uri = 'http://bbc.co.uk/news/'
        request = self.request(path=uri)
        self.assertEqual(request.get('RAW_URI'), uri)
        self.assertEqual(request.path, '/news/')
        self.assertEqual(request.absolute_uri(), uri)

    def test_is_secure(self):
        request = self.request(https=True)
        self.assertTrue(request.is_secure)
        self.assertEqual(request.environ['HTTPS'], 'on')
        self.assertEqual(request.environ['wsgi.url_scheme'], 'https')

    def test_get_host(self):
        request = self.request(headers=[('host', 'blaa.com')])
        self.assertEqual(request.get_host(), 'blaa.com')

    def test_full_path(self):
        request = self.request(headers=[('host', 'blaa.com')])
        self.assertEqual(request.full_path(), '/')
        self.assertEqual(request.full_path('/foo'), '/foo')

    def test_full_path_query(self):
        request = self.request(path='/bla?path=foo&id=5')
        self.assertEqual(request.path, '/bla')
        self.assertEqual(request.url_data, {'path': 'foo', 'id': '5'})
        self.assertEqual(request.full_path(), '/bla')
        # self.assertTrue(request.full_path() in ('/bla?path=foo&id=5',
        #                                         '/bla?id=5&path=foo'))
        self.assertEqual(request.full_path(g=7), '/bla?g=7')

    def test_url_handling(self):
        target = '/\N{SNOWMAN}'
        request = self.request(path=target)
        path = urlparse(request.path).path
        self.assertEqual(path, target)

    def testResponse200(self):
        r = wsgi.WsgiResponse(200)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.status, '200 OK')
        self.assertEqual(r.content, ())
        self.assertFalse(r.is_streamed)
        self.assertFalse(r.started)
        self.assertEqual(list(r), [])
        self.assertTrue(r.started)
        self.assertEqual(str(r), r.status)
        self.assertTrue(repr(r))

    def testResponse500(self):
        r = wsgi.WsgiResponse(500, content=b'A critical error occurred')
        self.assertEqual(r.status_code, 500)
        self.assertEqual(r.status, '500 Internal Server Error')
        self.assertEqual(r.content, (b'A critical error occurred',))
        self.assertFalse(r.is_streamed)
        self.assertFalse(r.started)
        self.assertEqual(list(r), [b'A critical error occurred'])
        self.assertTrue(r.started)

    def testStreamed(self):
        stream = ('line {0}\n'.format(l+1) for l in range(10))
        r = wsgi.WsgiResponse(content=stream)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.status, '200 OK')
        self.assertEqual(r.content, stream)
        self.assertTrue(r.is_streamed)
        data = []
        for l, a in enumerate(r):
            data.append(a)
            self.assertTrue(r.started)
            self.assertEqual(a, ('line {0}\n'.format(l+1)).encode('utf-8'))
        self.assertEqual(len(data), 10)

    def testForCoverage(self):
        r = wsgi.WsgiResponse(environ={'PATH_INFO': 'bla/'})
        self.assertEqual(r.path, 'bla/')
        self.assertEqual(r.connection, None)
        self.assertEqual(r.content, ())
        self.assertEqual(list(r), [])
        self.assertRaises(RuntimeError, list, r)

    def test_parse_authorization_header(self):
        parse = wsgi.parse_authorization_header
        self.assertEqual(parse(''), None)
        self.assertEqual(parse('csdcds'), None)
        self.assertEqual(parse('csdcds cbsdjchbjsc'), None)
        self.assertEqual(parse('basic cbsdjcbsjchbsd'), None)
        auths = http.HTTPBasicAuth('pippo', 'pluto').header()
        self.assertTrue(parse(auths).authenticated({}, 'pippo', 'pluto'))

    def testCookies(self):
        response = wsgi.WsgiResponse()
        expires = datetime.now() + timedelta(seconds=3600)
        response.set_cookie('bla', expires=expires)
        self.assertTrue('bla' in response.cookies)

    def testDeleteCookie(self):
        response = wsgi.WsgiResponse()
        response.delete_cookie('bla')
        self.assertTrue('bla' in response.cookies)

    def test_far_expiration(self):
        "Cookie will expire when an distant expiration time is provided"
        response = wsgi.WsgiResponse()
        response.set_cookie('datetime', expires=datetime(2028, 1, 1, 4, 5, 6))
        datetime_cookie = response.cookies['datetime']
        self.assertEqual(datetime_cookie['expires'],
                         'Sat, 01-Jan-2028 04:05:06 GMT')

    def test_max_age_expiration(self):
        "Cookie will expire if max_age is provided"
        response = wsgi.WsgiResponse()
        response.set_cookie('max_age', max_age=10)
        max_age_cookie = response.cookies['max_age']
        self.assertEqual(max_age_cookie['max-age'], 10)
        self.assertEqual(max_age_cookie['expires'],
                         cookie_date(time.time()+10))

    def test_httponly_cookie(self):
        response = wsgi.WsgiResponse()
        response.set_cookie('example', httponly=True)
        example_cookie = response.cookies['example']
        # A compat cookie may be in use -- check that it has worked
        # both as an output string, and using the cookie attributes
        self.assertTrue('; httponly' in str(example_cookie).lower())
        self.assertTrue(example_cookie['httponly'])

    def test_headers(self):
        response = wsgi.WsgiResponse(200)
        response['content-type'] = 'text/plain'
        self.assertTrue('content-type' in response)
        self.assertTrue(response.has_header('content-type'))
        self.assertEqual(response['content-type'], 'text/plain')

    def testBuildWsgiApp(self):
        appserver = wsgi.WSGIServer()
        self.assertEqual(appserver.name, 'wsgi')
        self.assertEqual(appserver.cfg.callable, None)

    def testWsgiHandler(self):
        hnd = wsgi.WsgiHandler(middleware=(wsgi.authorization_middleware,))
        self.assertEqual(len(hnd.middleware), 1)
        hnd2 = pickle.loads(pickle.dumps(hnd))
        self.assertEqual(len(hnd2.middleware), 1)

    def testHttpBinServer(self):
        from examples.httpbin.manage import server
        app = server(bind='127.0.0.1:0')
        pickle.loads(pickle.dumps(app))

    def test_clean_path_middleware(self):
        url = 'bla//foo'
        try:
            wsgi.clean_path_middleware({'PATH_INFO': url,
                                        'QUERY_STRING': 'page=1'}, None)
        except pulsar.HttpRedirect as e:
            url = e.headers[0][1]
            self.assertEqual(url, '/bla/foo?page=1')

    def test_handle_wsgi_error(self):

        def handler(request, exc):
            return 'exception: %s' % exc

        environ = wsgi.test_wsgi_environ(
            extra={'error.handler': handler})
        try:
            raise ValueError('just a test')
        except ValueError as exc:
            response = wsgi.handle_wsgi_error(environ, exc)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.content, (b'exception: just a test',))

    def test_handle_wsgi_error_debug(self):
        cfg = self.cfg.copy()
        cfg.set('debug', True)
        environ = wsgi.test_wsgi_environ(extra={'pulsar.cfg': cfg})
        try:
            raise ValueError('just a test for debug wsgi error handler')
        except ValueError as exc:
            response = wsgi.handle_wsgi_error(environ, exc)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.content_type, None)
        self.assertEqual(len(response.content), 1)

    def test_handle_wsgi_error_debug_html(self):
        cfg = self.cfg.copy()
        cfg.set('debug', True)
        headers = [('Accept', '*/*')]
        environ = wsgi.test_wsgi_environ(extra={'pulsar.cfg': cfg},
                                         headers=headers)
        try:
            raise ValueError('just a test for debug wsgi error handler')
        except ValueError as exc:
            response = wsgi.handle_wsgi_error(environ, exc)
        self.assertEqual(response.status_code, 500)
        html = response.content[0]
        self.assertEqual(response.content_type, 'text/html')
        self.assertTrue(html.startswith(b'<!DOCTYPE html>'))
        self.assertTrue(b'<title>500 Internal Server Error</title>' in html)

    async def test_wsgi_handler_404(self):
        start = mock.MagicMock()
        handler = wsgi.WsgiHandler()
        environ = self.request().environ
        response = await handler(environ, start)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(start.call_count, 1)

    def test_request_redirect(self):
        request = self.request()
        response = request.redirect('/foo')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['location'], '/foo')
        response = request.redirect('/foo2', permanent=True)
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response['location'], '/foo2')
