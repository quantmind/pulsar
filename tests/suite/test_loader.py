'''Tests the test suite loader.'''
import os
import unittest
import asyncio

from pulsar import get_application


class TestTestLoader(unittest.TestCase):

    @asyncio.coroutine
    def test_testsuite(self):
        app = yield from get_application('test')
        self.assertTrue(app.cfg.script)
        # self.assertEqual(app.script, sys.argv[0])
        self.assertEqual(os.path.dirname(app.cfg.script), app.root_dir)
        self.assertEqual(app.cfg.test_modules, ['tests', 'examples'])

    @asyncio.coroutine
    def test_load_pulsar_tests(self):
        app = yield from get_application('test')
        modules = dict(app.loader.test_files())
        self.assertTrue(modules)
        self.assertFalse('httpbin' in modules)
        self.assertTrue('echo' in modules)
        self.assertFalse('djchat' in modules)
        self.assertTrue('djchat.app' in modules)
        self.assertTrue('djchat.pulse' in modules)
        self.assertFalse('async' in modules)
        self.assertTrue('async.actor' in modules)

    @asyncio.coroutine
    def test_sorted_tags(self):
        app = yield from get_application('test')
        modules = app.loader.test_files()
        self.assertTrue(modules)
        tags = [m[0] for m in modules]
        self.assertEqual(tags, sorted(tags))

    @asyncio.coroutine
    def test_load_tags1(self):
        app = yield from get_application('test')
        modules = dict(app.loader.test_files(('suite',)))
        self.assertEqual(len(modules), 4)

    @asyncio.coroutine
    def test_load_exclude(self):
        app = yield from get_application('test')
        modules = dict(app.loader.test_files(
            exclude=('stores', 'apps.pubsub')))
        self.assertFalse('stores.lock' in modules)
        self.assertFalse('stores.parser' in modules)
        self.assertFalse('apps.pubsub' in modules)
        self.assertTrue('apps.greenio' in modules)
