import sys
from datetime import timedelta

from pulsar.apps.test import unittest
from pulsar.utils.pep import raise_error_trace


class TestMiscellaneous(unittest.TestCase):

    def test_raise_error_trace(self):
        self.assertRaises(RuntimeError, raise_error_trace, RuntimeError(),
                          None)
        try:
            raise RuntimeError('bla')
        except Exception:
            exc_info = sys.exc_info()
        self.assertRaises(RuntimeError, raise_error_trace, exc_info[1],
                          exc_info[2])
