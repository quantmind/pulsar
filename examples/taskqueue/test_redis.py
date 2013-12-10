'''Tests the taskqueue redis backend.'''
try:
    import stdnet
except ImportError:
    stdnet = None

from pulsar.apps.test import unittest

from . import test_pulsards


class TestRedisTaskQueueOnThread(test_pulsards.TestTaskQueueOnThread):
    #schedule_periodic = False

    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'


class TestRedisTaskQueueOnProcess(test_pulsards.TestTaskQueueOnProcess):

    @classmethod
    def task_backend(cls):
        return cls.cfg.backend_server or 'redis://127.0.0.1:6379'
