'''Tests the tools and utilities in pulsar.utils.'''
from pulsar.utils.pep import zip
from pulsar.utils.structures import Skiplist
from pulsar.apps.test import unittest, populate


class TestSkiplist(unittest.TestCase):
    skiplist = Skiplist

    def random(self, size=100, score_min=-10, score_max=10):
        scores = populate('float', size, score_min, score_max)
        data = populate('string', size, min_len=5, max_len=10)
        return self.skiplist(zip(scores, data))

    def test_extend(self):
        sl = self.skiplist()
        sl.extend([(94, 'bla'), (-5, 'foo')])
        self.assertEqual(len(sl), 2)

    def test_count(self):
        sl = self.skiplist()
        self.assertEqual(sl.count(-2, 2), 0)
        sl.insert(1, 'bla')
        self.assertEqual(sl.count(-2, 2), 1)
        sl = self.random()
        self.assertEqual(sl.count(float('-inf'), float('+inf')), 100)
        n = 0
        for score, _ in sl:
            n += int(score >= -5 and score <= 5)
        self.assertEqual(sl.count(-5, 5), n)
        #
        # test exclusive
        sl = self.skiplist(((1, 'a1'), (2, 'a2'), (3, 'a3')))
        self.assertEqual(sl.count(1, 2), 2)
        self.assertEqual(sl.count(1, 2, include_min=False), 1)
        self.assertEqual(sl.count(1, 2, include_max=False), 1)
        self.assertEqual(sl.count(1, 2, include_min=False,
                                  include_max=False), 0)
