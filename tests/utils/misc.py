import sys
import unittest
from datetime import timedelta

from pulsar.utils.pep import reraise


class TestMiscellaneous(unittest.TestCase):

    def test_reraise(self):
        self.assertRaises(RuntimeError, reraise, RuntimeError(), None)
        try:
            raise RuntimeError('bla')
        except Exception:
            exc_info = sys.exc_info()
        self.assertRaises(RuntimeError, reraise, exc_info[1], exc_info[2])
