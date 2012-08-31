'''Config and Setting classes'''
import pickle

from pulsar import get_actor, Config
from pulsar.apps.test import unittest


def worker_task(worker):
    return worker

class TestConfig(unittest.TestCase):
    
    def testFunction(self):
        cfg = Config()
        worker = get_actor()
        self.assertTrue(cfg.arbiter_task)
        self.assertEqual(cfg.arbiter_task(worker), None)
        cfg.set('arbiter_task', worker_task)
        self.assertEqual(cfg.arbiter_task(worker), worker)
        cfg1 = pickle.loads(pickle.dumps(cfg))
        self.assertEqual(cfg1.arbiter_task(worker), worker)
        
    def testFunctionFromConfigFile(self):
        worker = get_actor()
        cfg = Config()
        self.assertEqual(cfg.worker_task(worker), None)
        self.assertTrue(cfg.import_from_module(__file__))
        self.assertEqual(cfg.worker_task(worker), worker)
        cfg1 = pickle.loads(pickle.dumps(cfg))
        self.assertEqual(cfg1.worker_task(worker), worker)
        
    def testBadConfig(self):
        cfg = Config()
        self.assertEqual(cfg.import_from_module('foo/bla/cnkjnckjcn.py'), [])
        cfg.set('config', None)
        self.assertEqual(cfg.config, None)
        cfg = Config(exclude=['config'])
        self.assertEqual(cfg.config, None)
        
        
        