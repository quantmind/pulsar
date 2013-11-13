'''Tests the tools and utilities in pulsar.utils.'''
from pulsar.utils.structures import Skiplist
from pulsar.apps.test import unittest

class TestSkiplist(unittest.TestCase):
    skiplist = Skiplist

    def test_extend(self):
        sl = self.skiplist()
        sl.extend((94, 'bla', -5, 'foo'))
        self.assertEqual(len(sl), 2)
