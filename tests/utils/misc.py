import sys

from pulsar.apps.test import unittest
from pulsar.utils.pep import raise_error_trace


class TestMiscellaneous(unittest.TestCase):

    def test_raise_error_trace(self):
        self.assertRaises(ValueError, raise_error_trace, ValueError, None)
        try:
            raise ValueError('bla')
        except Exception:
            exc_info = sys.exc_info()
        self.assertRaises(ValueError, raise_error_trace, exc_info[1],
                          exc_info[2])
