'''Tests the tools and utilities in pulsar.utils.'''
import unittest
import pickle

from pulsar.utils.structures import (MultiValueDict, merge_prefix, deque,
                                     AttributeDictionary)


class TestMultiValueDict(unittest.TestCase):

    def testConstructor(self):
        m = MultiValueDict()
        self.assertEqual(len(m), 0)
        #
        m = MultiValueDict({'bla': 3})
        self.assertEqual(len(m), 1)
        self.assertEqual(m['bla'], 3)
        #
        m = MultiValueDict({'bla': (3, 78), 'foo': 'ciao'})
        self.assertEqual(len(m), 2)
        self.assertEqual(m['bla'], [3, 78])
        self.assertEqual(m['foo'], 'ciao')
        #
        m = MultiValueDict({'bla': [3, 78], 'foo': (v for v in (1, 2))})
        self.assertEqual(m['bla'], [3, 78])
        self.assertEqual(m['foo'], [1, 2])

    def testset(self):
        m = MultiValueDict()
        m['bla'] = 5
        m['bla'] = 89
        self.assertEqual(m['bla'], [5, 89])
        m['foo'] = 'pippo'
        self.assertEqual(m['foo'], 'pippo')
        return m

    def testextra(self):
        m = MultiValueDict()
        m.setdefault('bla', 'foo')
        self.assertEqual(m['bla'], 'foo')
        m['bla'] = 'ciao'
        self.assertEqual(m['bla'], ['foo', 'ciao'])

    def testget(self):
        m = self.testset()
        self.assertEqual(m.get('sdjcbhjcbh'), None)
        self.assertEqual(m.get('sdjcbhjcbh', 'ciao'), 'ciao')

    def testupdate(self):
        m = self.testset()
        m.update({'bla': 'star', 5: 'bo'})
        self.assertEqual(m['bla'], [5, 89, 'star'])
        self.assertEqual(m[5], 'bo')

    def test_iterators(self):
        m = self.testset()
        d = dict(m.items())
        self.assertEqual(d['bla'], [5, 89])
        self.assertEqual(d['foo'], 'pippo')
        l = list(m.values())
        self.assertEqual(len(l), 2)
        self.assertTrue([5, 89] in l)
        self.assertTrue('pippo' in l)

    def testlists(self):
        m = self.testset()
        items = dict(m.lists())
        self.assertEqual(len(items), 2)
        for k, v in items.items():
            self.assertTrue(isinstance(v, list))
        self.assertEqual(items['bla'], [5, 89])
        self.assertEqual(items['foo'], ['pippo'])

    def testCopy(self):
        m = self.testset()
        m2 = m.copy()
        self.assertEqual(m2['bla'], [5, 89])
        self.assertEqual(m2['foo'], 'pippo')
        self.assertEqual(m2.getlist('foo'), ['pippo'])

    def testPop(self):
        m = self.testset()
        self.assertRaises(KeyError, m.pop, 'jhsdbcjcd')
        self.assertEqual(m.pop('skcbnskcbskcbd', 'ciao'), 'ciao')
        self.assertEqual(m.pop('foo'), 'pippo')
        self.assertRaises(KeyError, m.pop, 'foo')
        self.assertEqual(m.pop('bla'), [5, 89])
        self.assertFalse(m)

    def test_to_dict(self):
        m = MultiValueDict((('id', 1), ('id', 2)))
        self.assertEqual(len(m), 1)
        self.assertEqual(m['id'], [1, 2])
        d = dict(m)
        self.assertEqual(d, {'id': [1, 2]})


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


class TestFunctions(unittest.TestCase):

    def test_merge_prefix(self):
        d = deque([b'abc', b'de', b'fghi', b'j'])
        merge_prefix(d, 5)
        self.assertEqual(d, deque([b'abcde', b'fghi', b'j']))
        d = deque([b'abc', b'de', b'fghi', b'j'])
        merge_prefix(d, 4)
        self.assertEqual(d, deque([b'abcd', b'e', b'fghi', b'j']))
        merge_prefix(d, 7)
        self.assertEqual(d, deque([b'abcdefg', b'hi', b'j']))
        merge_prefix(d, 3)
        self.assertEqual(d, deque([b'abc', b'defg', b'hi', b'j']))
        merge_prefix(d, 100)
        self.assertEqual(d, deque([b'abcdefghij']))
