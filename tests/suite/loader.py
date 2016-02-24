'''Tests the test suite loader.'''
import os
import unittest
import asyncio

from pulsar import get_application
from pulsar.apps.test import TestLoader


class TestTestLoader(unittest.TestCase):

    @asyncio.coroutine
    def test_testsuite(self):
        app = yield from get_application('test')
        self.assertTrue(app.cfg.script)
        # self.assertEqual(app.script, sys.argv[0])
        self.assertEqual(os.path.dirname(app.cfg.script), app.root_dir)
        self.assertEqual(app.cfg.modules, ('tests',
                                           ('examples', 'tests'),
                                           ('examples', 'test_*')))

    @asyncio.coroutine
    def test_load_pulsar_tests(self):
        app = yield from get_application('test')
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        self.assertEqual(loader.modules, [('tests', None, None),
                                          ('examples', 'tests', None),
                                          ('examples', 'test_*', None)])
        modules = dict(loader.testmodules())
        self.assertTrue(modules)
        self.assertFalse('httpbin' in modules)
        self.assertTrue('echo' in modules)
        self.assertTrue('djchat' in modules)
        self.assertTrue('djchat.app' in modules)
        self.assertTrue('djchat.pulse' in modules)
        self.assertTrue('async' in modules)
        self.assertTrue('suite.single' in modules)

    @asyncio.coroutine
    def test_sorted_tags(self):
        app = yield from get_application('test')
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = list(loader.testmodules())
        self.assertTrue(modules)
        tags = [m[0] for m in modules]
        self.assertEqual(tags, sorted(tags))

    @asyncio.coroutine
    def test_load_tags1(self):
        app = yield from get_application('test')
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = dict(loader.testmodules(('suite',)))
        self.assertEqual(len(modules), 6)

    @asyncio.coroutine
    def test_load_exclude(self):
        app = yield from get_application('test')
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = dict(loader.testmodules(
            exclude_tags=('taskqueue', 'apps.pubsub')))
        self.assertTrue(modules)
        for module in modules:
            self.assertTrue('taskqueue' not in module)
            self.assertTrue('apps.pubsub' not in module)
