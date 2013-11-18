from random import randint

from pulsar.utils.structures import Zset
from pulsar.apps.test import unittest, populate


class TestZset(unittest.TestCase):
    zset = Zset

    def random(self):
        string = populate('string', size=100)
        values = populate('float', size=100, min=-10, max=10)
        s = self.zset()
        s.update(zip(values, string))
        return s

    def test_add(self):
        s = self.zset()
        s.add(3, 'ciao')
        s.add(4, 'bla')
        self.assertEqual(len(s), 2)
        s.add(-1, 'bla')
        self.assertEqual(len(s), 2)
        data = list(s)
        self.assertEqual(data, ['bla', 'ciao'])

    def test_rank(self):
        s = self.zset()
        s.add(3, 'ciao')
        s.add(4, 'bla')
        s.add(2, 'foo')
        s.add(20, 'pippo')
        s.add(-1, 'bla')
        self.assertEqual(len(s), 4)
        self.assertEqual(s.rank('bla'), 0)
        self.assertEqual(s.rank('foo'), 1)
        self.assertEqual(s.rank('ciao'), 2)
        self.assertEqual(s.rank('pippo'), 3)
        self.assertEqual(s.rank('xxxx'), None)

    def test_update(self):
        s = self.random()
        self.assertTrue(s)
        prev = None
        for score, _ in s.items():
            if prev is not None:
                self.assertTrue(score>=prev)
            prev = score
        return s

    def test_remove(self):
        s = self.test_update()
        values = list(s)
        while values:
            index = randint(0, len(values)-1)
            val = values.pop(index)
            self.assertTrue(val in s)
            self.assertNotEqual(s.remove(val), None)
            self.assertFalse(val in s)
        self.assertFalse(s)

    def test_range(self):
        s = self.random()
        values = s.range(3,10)
        self.assertTrue(values)
