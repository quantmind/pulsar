#!/usr/bin/env python
#
# For the highest coverage run the following command
#
#    coverage run runtests.py --concurrency thread --profile
#
import sys

from pulsar.utils.path import Path
from pulsar.apps.test import TestSuite
from pulsar.apps.test.plugins import bench, profile
import pulsar.utils.settings.backend

Path(__file__).add2python('stdnet', 1, down=['python-stdnet'],
                          must_exist=False)

def run(**params):
    print(sys.version)
    TestSuite(description='Pulsar Asynchronous test suite',
              modules=('tests',
                       ('examples', 'tests'),
                       ('examples', 'test_*')),
              plugins=(bench.BenchMark(),
                       profile.Profile()),
              pidfile='test.pid',
              **params).start()

if __name__ == '__main__':
    run()
