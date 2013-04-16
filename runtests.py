#!/usr/bin/env python
#
# For the highest coverage run the following command
#
#    coverage run runtests.py --concurrency thread --profile 
#
import os
import sys

from pulsar.utils.path import Path
from pulsar.apps.test import TestSuite, TestOption
from pulsar.apps.test.plugins import bench, profile

path = Path()
if path.add2python('stdnet', 1, down=['python-stdnet'], must_exist=False):
    # stdnet available, we can test the taskqueue redis backend
    class TaskQueueRedis(TestOption):
        name = 'redis_server'
        flags = ['--redis-server']
        default = 'redis://127.0.0.1:6379?db=0&timeout=0'
        desc = 'Connection string to redis server for testing task queue'


if __name__ == '__main__':
    print(sys.version)
    TestSuite(description='Pulsar Asynchronous test suite',
              modules=('tests',
                       ('examples','tests'),
                       ('examples', 'test_*')),
              plugins=(bench.BenchMark(), profile.Profile()),
              pidfile='test.pid').start()
