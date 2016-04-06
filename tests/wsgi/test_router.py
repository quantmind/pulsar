'''Tests the wsgi middleware in pulsar.apps.wsgi'''
import unittest

import pulsar
from pulsar.apps.wsgi import Router, RouterParam, route, test_wsgi_environ

from examples.httpbin.manage import HttpBin


class TRouter(Router):
    random = RouterParam(6)


class Router2(Router):
    random1 = RouterParam(5)

    def get(self, request):
        return request.response


class HttpBin2(HttpBin):

    def gzip(self):
        pass    # switch off gzip handler, it is not a route anymore

    @route('get2')
    def get_get(self, request):    # override the get_get handler
        raise pulsar.Http404

    @route()
    def async(self, request):
        future = pulsar.Future()
        future._loop.call_later(0.5, lambda: future.set_result(['Hello!']))
        return future

    @route()
    def post_async(self, request):
        future = pulsar.Future()
        future._loop.call_later(0.5, lambda: future.set_result(['Hello!']))
        return future


class MyRouter(Router):

    @route('/<id>')
    def get_elem(self, request):
        return request.response

    @route('/<id>')
    def post_elem(self, request):
        return request.response


class HttpBin3(HttpBin):

    @route('new', position=0)
    def new(self, request):
        return self.info_data_response(request)

    @route(method='post', title='Returns POST data', position=-1)
    def post_post(self, request):
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

    def test_override(self):
        self.assertTrue('get_get' in HttpBin.rule_methods)
        self.assertEqual(HttpBin.rule_methods['get_get'][0].rule, 'get')
        self.assertTrue('get_get' in HttpBin2.rule_methods)
        self.assertEqual(HttpBin2.rule_methods['get_get'][0].rule, 'get2')
        # The position in the ordered dict should be the same too
        all = list(HttpBin.rule_methods)
        all2 = list(HttpBin2.rule_methods)
        self.assertEqual(all2.index('get_get'), all.index('get_get'))

    def test_override_change_position(self):
        self.assertTrue('post_post' in HttpBin.rule_methods)
        self.assertEqual(HttpBin.rule_methods['post_post'][0].rule, 'post')
        self.assertTrue('post_post' in HttpBin3.rule_methods)
        self.assertEqual(HttpBin3.rule_methods['post_post'][0].rule, 'post')
        # The position in the ordered dict should be the same too
        all = list(HttpBin.rule_methods)
        all3 = list(HttpBin3.rule_methods)
        self.assertEqual(all3.index('new'), 1)
        self.assertTrue(all3.index('post_post') < all.index('post_post'))

    def test_path_method(self):
        router = Router('/root',
                        Router('a', get=lambda r: ['route a']))
        self.assertEqual(router.path(), '/root')
        self.assertEqual(router.route.is_leaf, True)
        child, args = router.resolve('root/a')
        self.assertFalse(args)
        self.assertEqual(child.parent, router)
        self.assertEqual(child.path(), '/root/a')

    def test_router_count(self):
        self.assertTrue(HttpBin2.rule_methods)
        async = HttpBin2.rule_methods.get('async')
        self.assertTrue(async)
        self.assertEqual(async.method, 'get')
        self.assertEqual(str(async.rule), '/async')
        async = HttpBin2.rule_methods.get('post_async')
        self.assertTrue(async)
        self.assertEqual(async.method, 'post')
        self.assertEqual(str(async.rule), '/async')
        #
        router = HttpBin2('/')
        self.assertEqual(router.name, '')
        router = HttpBin2('/', name='root')
        self.assertEqual(router.name, 'root')
        async = router.get_route('async')
        self.assertTrue(async)
        # It has both get and post methods
        self.assertTrue(async.get)
        self.assertTrue(async.post)

    def test_router_child(self):
        router = TRouter('/', HttpBin2('bin'), random=9)
        self.assertEqual(router.name, '')
        self.assertEqual(len(router.routes), 1)
        self.assertEqual(router.random, 9)
        self.assertEqual(router.root, router)
        child = router.get_route('binx')
        self.assertFalse(child)
        child = router.get_route('bin')
        self.assertTrue(child)
        self.assertEqual(router.root, child.root)
        self.assertTrue(router.has_parent(router))
        self.assertFalse(router.has_parent(child))
        self.assertTrue(child.has_parent(router))

    def test_child_methods(self):
        router = TRouter('/', HttpBin2('bin'), name='home')
        self.assertTrue('random' in router.defaults)
        #
        self.assertEqual(router.name, 'home')
        child = router.get_route('bin')
        self.assertTrue(child)
        #
        async = router.get_route('async')
        self.assertFalse('random' in async.defaults)
        self.assertTrue(async)
        self.assertEqual(async.root, router)
        self.assertEqual(async.parent, child)
        self.assertEqual(async.random, 6)
        #
        # It has both get and post methods
        self.assertTrue(async.get)
        self.assertTrue(async.post)

    def test_rule(self):
        router = TRouter('/', HttpBin2('bin'))
        self.assertEqual(repr(router), '/')
        self.assertEqual(router.rule, router.full_route.rule)

    def test_remove_child(self):
        router = TRouter('/', HttpBin2('bin'))
        child = router.routes[0]
        self.assertEqual(child.path(), '/bin')
        self.assertEqual(child.parent, router)
        router.remove_child(child)
        self.assertFalse(router.routes)
        self.assertEqual(child.parent, None)

    def test_default(self):
        router = Router2('/', TRouter('foo'))
        self.assertTrue(router.get)
        foo = router.get_route('foo')
        self.assertRaises(AttributeError, lambda: foo.get)
        self.assertEqual(foo.random1, 5)
        router = Router2('/', TRouter('foo'), random1=10)
        foo = router.get_route('foo')
        self.assertRaises(AttributeError, lambda: foo.get)
        self.assertEqual(foo.random1, 10)

    def test_multiple_methods(self):
        router = MyRouter('/')
        child = router.get_route('elem')
        self.assertTrue(child)
        self.assertTrue(child.get.__name__, 'get_elem')
        self.assertTrue(child.post.__name__, 'post_elem')

    def test_response_wrapper(self):

        def response_wrapper(callable, response):
            raise pulsar.PermissionDenied('Test Response Wrapper')

        router = HttpBin('/', response_wrapper=response_wrapper)

        environ = test_wsgi_environ()
        try:
            router(environ, None)
        except pulsar.PermissionDenied as exc:
            self.assertEqual(str(exc), 'Test Response Wrapper')
        else:
            raise RuntimeError

        environ = test_wsgi_environ('/get')
        try:
            router(environ, None)
        except pulsar.PermissionDenied as exc:
            self.assertEqual(str(exc), 'Test Response Wrapper')
        else:
            raise RuntimeError
