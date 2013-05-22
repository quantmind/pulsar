'''Tests the taskqueue redis backend.'''
try:
    import stdnet
except ImportError:
    stdnet = None
    
from pulsar.apps.test import unittest
 
from . import test_local

raise ImportError

unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnThread(test_local.TestTaskQueueOnThread):
    #schedule_periodic = False
    
    @classmethod
    def task_backend(cls):
        return cls.cfg.redis_server


#unittest.skipUnless(stdnet, 'Requires python-stdnet')
#class TestRedisTaskQueueOnProcess(test_app.TestTaskQueueOnProcess):
#    
#    @classmethod
#    def task_backend(cls):
#        return cls.cfg.redis_server
