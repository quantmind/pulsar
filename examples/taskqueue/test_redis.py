'''Tests the taskqueue redis backend.'''
try:
    import stdnet
except ImportError:
    stdnet = None
    
from pulsar.apps.test import unittest
 
from . import test_local


def redis_pubsub_test(self, app):
    pubsub = app.backend.pubsub
    con_str = pubsub.backend.connection_string
    self.assertTrue('namespace=%s.' % app.name in con_str)
    self.assertFalse('name=%s' % app.name in con_str)
        
        
@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnThread(test_local.TestTaskQueueOnThread):
    #schedule_periodic = False
    
    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'
    
    def pubsub_test(self, app):
        redis_pubsub_test(self, app)


@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnProcess(test_local.TestTaskQueueOnProcess):
    
    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'
    
    def pubsub_test(self, app):
        redis_pubsub_test(self, app)
