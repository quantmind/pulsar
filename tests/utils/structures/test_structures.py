'''Tests the tools and utilities in pulsar.utils.'''
import unittest
import pickle

from pulsar.utils.structures import AttributeDictionary


class TestAttributeDictionary(unittest.TestCase):

    def testInit(self):
        self.assertRaises(TypeError, AttributeDictionary, {}, {})
        a = AttributeDictionary({'bla': 1}, foo='pippo')
        self.assertEqual(dict(a), {'bla': 1, 'foo': 'pippo'})
        self.assertEqual(len(a), 2)

    def testAssign(self):
        a = AttributeDictionary()
        a['ciao'] = 5
        self.assertEqual(a.ciao, 5)
        self.assertEqual(a['ciao'], 5)
        self.assertEqual(list(a.values()), [5])
        self.assertEqual(list(a.items()), [('ciao', 5)])

    def testCopy(self):
        a = AttributeDictionary(foo=5, bla='ciao')
        self.assertEqual(len(a), 2)
        b = a.copy()
        self.assertEqual(a, b)
        self.assertNotEqual(id(a), id(b))

    def __test_pickle(self):
        # TODO: this fails at times
        a = AttributeDictionary()
        a['ciao'] = 5
        b = pickle.dumps(a)
        c = pickle.loads(b)
        self.assertEqual(a, c)
