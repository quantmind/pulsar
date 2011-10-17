#!/usr/bin/env python
import os
import sys
from pulsar.apps.test import TestSuite

if __name__ == '__main__':
    TestSuite(description = 'Pulsar Asynchronous test suite',
              modules = ('tests',
                         ('examples','tests'))).start()
