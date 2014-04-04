'''Tests the taskqueue redis backend.'''
import unittest

from pulsar.apps.test import check_server

from . import test_pulsards

OK = check_server('redis')


@unittest.skipUnless(OK, 'Requires a running redis server')
class TestRedisTaskQueueOnThread(test_pulsards.TestTaskQueueOnThread):
    # schedule_periodic = False

    @classmethod
    def task_backend(cls):
        return 'redis://%s' % cls.cfg.redis_server


@unittest.skipUnless(OK, 'Requires a running redis server')
class TestRedisTaskQueueOnProcess(test_pulsards.TestTaskQueueOnProcess):

    @classmethod
    def task_backend(cls):
        return 'redis://%s' % cls.cfg.redis_server
