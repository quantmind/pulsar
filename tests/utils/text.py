'''Tests the tools and utilities in pulsar.utils.'''
import unittest

from pulsar.utils.log import lazy_string


class TestTextUtils(unittest.TestCase):

    def testLazy(self):
        @lazy_string
        def blabla(n):
            return 'AAAAAAAAAAAAAAAAAAAA %s' % n
        r = blabla(3)
        self.assertEqual(r.value, None)
        v = str(r)
        self.assertEqual(v, 'AAAAAAAAAAAAAAAAAAAA 3')
        self.assertEqual(r.value, v)
