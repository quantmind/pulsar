'''Tests the rpc middleware and utilities. It uses the calculator example.'''
import unittest
import asyncio

import pulsar
from pulsar import Config, get_actor
from pulsar.apps import MultiApp
from pulsar.apps.wsgi import WSGIServer

from tests.apps import dummy


class MultiWsgi(MultiApp):
    cfg = Config(bind=':0', rpc_bind=':0', bla='foo')

    def build(self):
        yield self.new_app(WSGIServer, callable=dummy)
        yield self.new_app(WSGIServer, 'rpc', callable=dummy)


class TestMultiApp(unittest.TestCase):

    def create(self, **params):
        # create the application
        return MultiWsgi(**params)

    def test_app(self):
        app = self.create(version='2.0')
        self.assertEqual(app.version, '2.0')
        self.assertTrue(app)
        apps = app.apps()
        self.assertEqual(len(apps), 2)
        self.assertEqual(apps[0].version, '2.0')
        self.assertEqual(apps[1].version, pulsar.__version__)

    def testConfig(self):
        app = self.create(bind=':9999', unz='whatz')
        self.assertTrue(app)
        self.assertEqual(len(app.cfg.params), 4)
        app.apps()
        self.assertEqual(len(app.cfg.params), 2)
        self.assertEqual(app.cfg.bla, 'foo')
        self.assertEqual(app.cfg.unz, 'whatz')
        self.assertEqual(app.cfg.bind, ':9999')

    def test_timeout(self):
        app = self.create(timeout=22, name='pippo')
        self.assertEqual(app.name, 'pippo')
        self.assertEqual(app.cfg.timeout, 22)
        apps = app.apps()
        self.assertEqual(app.cfg.timeout, 22)
        self.assertNotEqual(app.cfg.rpc_timeout, 22)
        app1 = apps[0]
        self.assertEqual(app1.name, 'pippo')
        self.assertEqual(app1.cfg.timeout, 22)
        app2 = apps[1]
        self.assertEqual(app2.name, 'rpc_pippo')
        self.assertNotEqual(app2.cfg.timeout, 22)
        self.assertEqual(app2.cfg.timeout,
                         app2.cfg.settings['timeout'].default)

    def testName(self):
        app = self.create()
        self.assertEqual(app.name, 'multiwsgi')
        app = self.create(name='bla')
        self.assertEqual(app.name, 'bla')
        apps = app.apps()
        self.assertEqual(apps[0].name, 'bla')
        self.assertEqual(apps[1].name, 'rpc_bla')

    @asyncio.coroutine
    def testInstall(self):
        arbiter = get_actor()
        app = self.create(name='pluto')
        self.assertTrue(app)
        self.assertFalse(arbiter.get_actor('pluto'))
        self.assertFalse(arbiter.get_actor('rpc_pluto'))
        # create the application
        yield from app.start()
        monitor1 = arbiter.get_actor('pluto')
        self.assertTrue(monitor1)
        self.assertTrue(monitor1.is_monitor())
        monitor2 = arbiter.get_actor('rpc_pluto')
        self.assertTrue(monitor2)
        self.assertTrue(monitor2.is_monitor())
        yield from monitor1.stop()
        yield from monitor2.stop()

    def test_config_copy(self):
        app = self.create()
        self.assertTrue(app)
        apps = app.apps()
        rpc = apps[1]
        cfg = rpc.cfg.copy()
        self.assertEqual(cfg.prefix, rpc.cfg.prefix)
        for name in cfg:
            self.assertTrue(name in rpc.cfg)
