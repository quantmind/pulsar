'''Tests the test suite loader.'''
import os
import unittest

from pulsar import get_application


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
        self.assertTrue('suite.loader' in modules)
        self.assertEqual(modules['suite.loader'][1], 'test_load_test_function')

    async def test_load_bad_test_function(self):
        app = await get_application('test')
        modules = dict(app.loader.test_files(
            ['suite.loader.load_test_function']))
        self.assertEqual(len(modules), 0)
