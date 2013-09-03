'''Tests the taskqueue redis backend.'''
try:
    import stdnet
except ImportError:
    stdnet = None
    
from pulsar.apps.test import unittest
from pulsar.utils.internet import parse_connection_string, get_connection_string
 
from . import test_local

@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnThread(test_local.TestTaskQueueOnThread):
    #schedule_periodic = False
    
    @classmethod
    def task_backend(cls):
        backend = cls.cfg.backend_server or 'redis://127.0.0.1'
        sheme, host, params = parse_connection_string(backend, 6379)
        params['name'] = cls.name()
        return get_connection_string(sheme, host, params)


@unittest.skipUnless(stdnet, 'Requires python-stdnet')
class TestRedisTaskQueueOnProcess(test_local.TestTaskQueueOnProcess):
    
    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'
