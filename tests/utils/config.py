'''Config and Setting classes'''
import os
import pickle
import tempfile

import pulsar
from pulsar import get_actor, Config
from pulsar.apps.test import unittest


def connection_made(conn):
    return conn

def post_fork(actor):
    return actor

class TestConfig(unittest.TestCase):
    
    def testFunction(self):
        cfg = Config()
        worker = get_actor()
        self.assertTrue(cfg.post_fork)
        self.assertEqual(cfg.post_fork(worker), None)
        cfg.set('post_fork', post_fork)
        self.assertEqual(cfg.post_fork(worker), worker)
        cfg1 = pickle.loads(pickle.dumps(cfg))
        self.assertEqual(cfg1.post_fork(worker), worker)
        
    def testFunctionFromConfigFile(self):
        worker = get_actor()
        cfg = Config()
        self.assertEqual(cfg.connection_made(worker), None)
        self.assertTrue(cfg.import_from_module(__file__))
        self.assertEqual(cfg.connection_made(worker), worker)
        cfg1 = pickle.loads(pickle.dumps(cfg))
        self.assertEqual(cfg1.connection_made(worker), worker)
        
    def testBadConfig(self):
        cfg = Config()
        self.assertEqual(cfg.import_from_module('foo/bla/cnkjnckjcn.py'), [])
        cfg.set('config', None)
        self.assertEqual(cfg.config, None)
        cfg = Config(exclude=['config'])
        self.assertEqual(cfg.config, None)
        
    def testDefaults(self):
        from pulsar.utils import config
        self.assertFalse(config.pass_through(None))
        cfg = Config()
        self.assertEqual(list(sorted(cfg)), list(sorted(cfg.settings)))
        def _():
            cfg.debug = 3
        self.assertRaises(AttributeError, _)
        #
        name = tempfile.mktemp()
        with open(name, 'w') as f:
            f.write('a')
        self.assertRaises(RuntimeError, cfg.import_from_module, name)
        os.remove(name)
        #
        name = '%s.py' % name
        with open(name, 'w') as f:
            f.write('a')
        self.assertRaises(RuntimeError, cfg.import_from_module, name)
        os.remove(name)
        
    def testSystem(self):
        from pulsar import system
        cfg = Config()
        self.assertEqual(cfg.uid, system.get_uid())
        self.assertEqual(cfg.gid, system.get_gid())
        self.assertEqual(cfg.proc_name, 'Pulsar')
        cfg.set('process_name', 'bla')
        self.assertEqual(cfg.proc_name, 'bla')
        
    def testValidation(self):
        self.assertEqual(pulsar.validate_list((1,2)), [1,2])
        self.assertRaises(TypeError, pulsar.validate_list, 'bla')
        self.assertEqual(pulsar.validate_string(b' bla  '), 'bla')
        self.assertEqual(pulsar.validate_string(None), None)
        self.assertRaises(TypeError, pulsar.validate_string, [])
        self.assertEqual(pulsar.validate_bool(True), True)
        self.assertEqual(pulsar.validate_bool('true '), True)
        self.assertEqual(pulsar.validate_bool(' false'), False)
        self.assertRaises(TypeError, pulsar.validate_bool, [])
        self.assertRaises(ValueError, pulsar.validate_bool, 'foo')
        
        