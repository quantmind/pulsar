from random import randint
import unittest

from pulsar.utils.structures import Skiplist
from pulsar.apps.test import populate


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

    def test_range_by_score(self):
        sl = self.skiplist()
        self.assertEqual(tuple(sl.range_by_score(float('-inf'), 5)), ())
        sl.insert(1, 'bla')
        self.assertEqual(tuple(sl.range_by_score(-3, 5)), ('bla',))
        sl.insert(1.5, 'foo')
        sl.insert(0.3, 'pippo')
        self.assertEqual(tuple(sl.range_by_score(0.9, 1.1)), ('bla',))
        self.assertEqual(tuple(sl.range_by_score(1, 2)), ('bla', 'foo'))
        self.assertEqual(tuple(sl.range_by_score(1, 2, include_min=False)),
                         ('foo',))
        self.assertEqual(tuple(sl.range_by_score(1, 1.5)), ('bla', 'foo'))
        self.assertEqual(tuple(sl.range_by_score(1, 1.5, include_max=False)),
                         ('bla',))
        self.assertEqual(tuple(sl.range_by_score(-1, 2, start=1)),
                         ('bla', 'foo'))
        self.assertEqual(tuple(sl.range_by_score(-1, 2, start=1, num=1)),
                         ('bla',))

    def test_remove_range(self):
        sl = self.skiplist()
        self.assertEqual(sl.remove_range(0, 3), 0)
        sl.insert(1, 'bla')
        self.assertEqual(sl.remove_range(0, 3), 1)
        sl = self.random(10)
        li = list(sl)
        lir = li[:4] + li[7:]
        self.assertEqual(sl.remove_range(4, 7), 3)
        li2 = list(sl)
        self.assertEqual(li2, lir)
        #
        sl = self.random()
        li = list(sl)
        lir = li[:5] + li[-8:]
        self.assertEqual(sl.remove_range(5, -8), len(li[5:-8]))
        li2 = list(sl)
        self.assertEqual(li2, lir)
        #
        sl = self.random()
        c = 0
        while sl:
            c += 1
            index = randint(0, len(sl)-1)
            self.assertEqual(sl.remove_range(index, index+1), 1)
        self.assertEqual(c, 100)

    def test_remove_range_by_score(self):
        sl = self.skiplist()
        self.assertEqual(sl.remove_range_by_score(0, 3), 0)
        sl.insert(1, 'bla')
        self.assertEqual(sl.remove_range_by_score(0, 3), 1)
