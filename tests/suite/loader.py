'''Tests the test suite loader.'''
import os
import sys
import time
from threading import current_thread

import pulsar
from pulsar.apps.test import unittest, TestLoader


class TestTestLoader(unittest.TestCase):

    def test_testsuite(self):
        app = pulsar.get_actor().app
        self.assertTrue(app.script)
        #self.assertEqual(app.script, sys.argv[0])
        self.assertEqual(os.path.dirname(app.script), app.root_dir)
        self.assertEqual(app.cfg.modules, ('tests',
                                          ('examples', 'tests'),
                                          ('examples', 'test_*')))

    def test_load_pulsar_tests(self):
        app = pulsar.get_actor().app
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        self.assertEqual(loader.modules, [('tests', None, None),
                                          ('examples', 'tests', None),
                                          ('examples', 'test_*', None)])
        modules = dict(loader.testmodules())
        self.assertTrue(modules)
        self.assertFalse('httpbin' in modules)
        self.assertTrue('echo' in modules)
        self.assertTrue('djangoapp' in modules)
        self.assertTrue('djangoapp.app' in modules)
        self.assertTrue('djangoapp.pulse' in modules)
        self.assertTrue('async' in modules)
        self.assertTrue('suite.single' in modules)

    def test_sorted_tags(self):
        app = pulsar.get_actor().app
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = list(loader.testmodules())
        self.assertTrue(modules)
        tags = [m[0] for m in modules]
        self.assertEqual(tags, sorted(tags))

    def test_load_tags1(self):
        app = pulsar.get_actor().app
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = dict(loader.testmodules(('suite',)))
        self.assertEqual(len(modules), 6)

    def test_load_exclude(self):
        app = pulsar.get_actor().app
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = dict(loader.testmodules(exclude_tags=
                            ('taskqueue', 'apps.pubsub')))
        self.assertTrue(modules)
        for module in modules:
            self.assertTrue('taskqueue' not in module)
            self.assertTrue('apps.pubsub' not in module)

    def __test_djangoapp_tags(self):
        #TODO Fix this
        app = pulsar.get_actor().app
        loader = TestLoader(app.root_dir, app.cfg.modules, app.runner)
        modules = dict(loader.testmodules(('djangoapp',)))
        self.assertEqual(len(modules), 3)
