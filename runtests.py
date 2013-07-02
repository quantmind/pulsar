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
from pulsar.apps.test.plugins import bench, profile, cov

path = Path()
if path.add2python('stdnet', 1, down=['python-stdnet'], must_exist=False):
    import pulsar.apps.test.backend


if __name__ == '__main__':
    print(sys.version)
    TestSuite(description='Pulsar Asynchronous test suite',
              modules=('tests',
                       ('examples','tests'),
                       ('examples', 'test_*')),
              plugins=(bench.BenchMark(),
                       profile.Profile(),
                       cov.Coverage()),
              pidfile='test.pid').start()
