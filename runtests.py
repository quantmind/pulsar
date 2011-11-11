#!/usr/bin/env python
import os
import sys
from pulsar.apps.test import TestSuite
from pulsar.apps.test.plugins import bench


if __name__ == '__main__':
    TestSuite(description = 'Pulsar Asynchronous test suite',
              modules = ('tests',
                         ('examples','tests')),
              plugins = (bench.BenchMark(),)).start()
