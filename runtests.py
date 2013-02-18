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

if __name__ == '__main__':
    print(sys.version)
    TestSuite(description='Pulsar Asynchronous test suite',
              modules=('tests', ('examples','tests')),
              plugins=(bench.BenchMark(), profile.Profile()),
              pidfile='test.pid').start()
