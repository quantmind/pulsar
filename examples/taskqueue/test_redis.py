'''Tests the taskqueue redis backend.'''
try:
    import stdnet
except ImportError:
    stdnet = None
    
from pulsar.apps.test import unittest
 
from . import test_local

@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnThread(test_local.TestTaskQueueOnThread):
    #schedule_periodic = False
    
    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'


@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnProcess(test_local.TestTaskQueueOnProcess):
    
    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'
