'''Tests the test suite loader.'''
import os
import unittest

from pulsar import get_application
from pulsar.utils.system import platform


class TestTestLoader(unittest.TestCase):

    async def test_testsuite(self):
        app = await get_application('test')
        self.assertTrue(app.cfg.script)
        # self.assertEqual(app.script, sys.argv[0])
        self.assertEqual(os.path.dirname(app.cfg.script), app.root_dir)
        self.assertEqual(app.cfg.test_modules, ['tests', 'examples'])

    async def test_load_pulsar_tests(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files())
        self.assertTrue(modules)
        self.assertFalse('httpbin' in modules)
        self.assertTrue('echo' in modules)
        self.assertFalse('async' in modules)

    async def test_sorted_tags(self):
        app = await get_application('test')
        modules = app.loader.test_files()
        self.assertTrue(modules)
        tags = [m[0] for m in modules]
        self.assertEqual(tags, sorted(tags))

    async def test_load_tags1(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(('suite',)))
        self.assertEqual(len(modules), 4)

    async def test_load_exclude(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(
            exclude=('stores', 'apps.pubsub')))
        self.assertFalse('stores.lock' in modules)
        self.assertFalse('stores.parser' in modules)
        self.assertFalse('apps.pubsub' in modules)
        self.assertTrue('apps.greenio' in modules)

    async def test_load_test_function(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(
            ['suite.loader.test_load_test_function']))
        self.assertEqual(len(modules), 1)
        module = modules.get('suite.loader.test_load_test_function')
        self.assertTrue(module)
        self.assertEqual(module[1], 'test_load_test_function')

    async def test_load_bad_test_function(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(
            ['suite.loader.load_test_function']))
        self.assertEqual(len(modules), 0)

    async def test_load_two_labels(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(
            ['wsgi.route', 'wsgi.router']))
        self.assertEqual(len(modules), 2)
        self.assertTrue('wsgi.route' in modules)
        self.assertTrue('wsgi.router' in modules)

    @unittest.skipUnless(platform.type != 'win', 'This fails in windows')
    async def test_load_http_client_test(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(
            ['http.client.test_home_page_head']))
        self.assertEqual(len(modules), 1)
        load = modules['http.client.test_home_page_head']
        self.assertTrue(load[0].endswith('/tests/http/test_client.py'))
        self.assertEqual(load[1], 'test_home_page_head')

    async def test_load_http(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(['http']))
        self.assertEqual(len(modules), 10)
