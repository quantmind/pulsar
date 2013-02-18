#!/usr/bin/env python
#
# For the highest coverage run the following command
#
#    coverage run runtests.py --concurrency thread --profile 
#
import os
import sys
from pulsar.apps.test import TestSuite, TestPlugin
from pulsar.apps.test.plugins import bench, profile

class RedisServer(TestPlugin):
    name = "redis_server"
    flags = ["--redis-server"]
    desc = 'Back-end data server where to run redis tests.'
    default = 'redis://127.0.0.1:6349'

if __name__ == '__main__':
    print(sys.version)
    TestSuite(description='Pulsar Asynchronous test suite',
              modules=('tests', ('examples','tests')),
              plugins=(bench.BenchMark(), profile.Profile(), RedisServer()),
              pidfile='test.pid').start()
