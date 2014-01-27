'''Tests the wsgi middleware in pulsar.apps.wsgi'''
import unittest

import pulsar
from pulsar.apps.wsgi import Router, RouterParam, route

from examples.httpbin.manage import HttpBin


class HttpBin2(HttpBin):

    def gzip(self):
        pass    # switch off gzip handler, it is not a route anymore

    @route('get2')
    def _get(self, request):    # override the _get handler
        raise pulsar.Http404

    @route('async', async=True)
    def test_async_route(self, request):
        yield 'Hello'

    @route('async', async=True, method='post')
    def test_async_route_post(self, request):
        yield 'Hello'


class HttpBin3(HttpBin):

    @route('new', position=0)
    def new(self, request):
        return self.info_data_response(request)

    @route('post', method='post', title='Returns POST data', position=-1)
    def _post(self, request):
        return self.info_data_response(request)


class TestRouter(unittest.TestCase):

    def router(self, path='/'):
        class testRouter(Router):
            response_content_types = RouterParam(('text/html',
                                                  'text/plain',
                                                  'application/json'))

            def get(self, request):
                return 'Hello World!'

            @route()
            def bla(self, request):
                return 'This is /bla route'

            @route('/foo')
            def xxx(self, request):
                return 'This is /foo route'

            @route()
            def post_pluto(self, request):
                return 'This is /pluto POST route'

        router = testRouter(path)
        self.assertEqual(len(router.routes), 3)
        return router

    def test_router(self):
        router = self.router()
        self.assertEqual(router.route.path, '/')
        handler, urlargs = router.resolve('')
        self.assertEqual(handler, router)
        self.assertEqual(urlargs, {})
        #
        handler, urlargs = router.resolve('bla')
        self.assertNotEqual(handler, router)
        self.assertEqual(urlargs, {})

    def test_derived(self):
        self.assertTrue('gzip' in HttpBin.rule_methods)
        self.assertFalse('gzip' in HttpBin2.rule_methods)

    def test_async_route(self):
        self.assertTrue('test_async_route' in HttpBin2.rule_methods)
        method = HttpBin2.rule_methods['test_async_route']
        self.assertEqual(method[2].get('async'), True)
        app = HttpBin2('/')
        router, args = app.resolve('async')
        self.assertFalse(args)
        get = router.get
        post = router.post

    def test_override(self):
        self.assertTrue('_get' in HttpBin.rule_methods)
        self.assertEqual(HttpBin.rule_methods['_get'][0].rule, 'get')
        self.assertTrue('_get' in HttpBin2.rule_methods)
        self.assertEqual(HttpBin2.rule_methods['_get'][0].rule, 'get2')
        # The position in the ordered dict should be the same too
        all = list(HttpBin.rule_methods)
        all2 = list(HttpBin2.rule_methods)
        self.assertEqual(all2.index('_get'), all.index('_get'))

    def test_override_change_position(self):
        self.assertTrue('_post' in HttpBin.rule_methods)
        self.assertEqual(HttpBin.rule_methods['_post'][0].rule, 'post')
        self.assertTrue('_get' in HttpBin3.rule_methods)
        self.assertEqual(HttpBin3.rule_methods['_post'][0].rule, 'post')
        # The position in the ordered dict should be the same too
        all = list(HttpBin.rule_methods)
        all3 = list(HttpBin3.rule_methods)
        self.assertEqual(all3.index('new'), 1)
        self.assertTrue(all3.index('_post') < all.index('_post'))

    def test_accept(self):
        router = self.router()
        self.assertEqual(router.accept_content_type('text/html'), 'text/html')
        self.assertEqual(router.accept_content_type('text/*'), 'text/html')
        self.assertEqual(router.accept_content_type('text/plain'),
                         'text/plain')
        self.assertEqual(router.accept_content_type('*/*'), 'text/html')
        self.assertEqual(router.accept_content_type('application/*'),
                         'application/json')
        self.assertEqual(router.accept_content_type('application/json'),
                         'application/json')
        self.assertEqual(router.accept_content_type('application/javascript'),
                         None)
