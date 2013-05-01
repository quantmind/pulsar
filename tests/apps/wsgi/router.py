'''Tests the wsgi middleware in pulsar.apps.wsgi'''
import pulsar
from pulsar.apps.wsgi import Router, route
from pulsar.apps.test import unittest

from examples.httpbin.manage import HttpBin

class HttpBin2(HttpBin):
    
    def gzip(self):
        pass    # switch off gzip handler
    
    @route('async', async=True)
    def teast_async_route(self, request):
        yield  'Hello'
        
    @route('async', async=True, method='post')
    def teast_async_route_post(self, request):
        yield  'Hello'


class TestRouter(unittest.TestCase):
    
    def router(self, path='/'):
        class testRouter(Router):
            
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
        self.assertTrue('teast_async_route' in HttpBin2.rule_methods)
        method = HttpBin2.rule_methods['teast_async_route']
        self.assertEqual(method[2].get('async'), True)
        app = HttpBin2('/')
        router, args = app.resolve('async')
        self.assertFalse(args)
        get = router.get
        post = router.post
        self.assertTrue(get.async)
        self.assertTrue(post.async)
        
    