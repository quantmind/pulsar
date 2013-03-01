'''Tests the wsgi middleware in pulsar.apps.wsgi'''
import pulsar
from pulsar.apps.wsgi import Router, route
from pulsar.apps.test import unittest


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
            
        self.assertEqual(len(testRouter.routes), 3)
        return testRouter(path)
    
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