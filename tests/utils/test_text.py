'''Tests the tools and utilities in pulsar.utils.'''
import unittest

from pulsar.utils.log import lazy_string


@lazy_string
def blabla(n):
    return 'AAAAAAAAAAAAAAAAAAAA %s' % n


class TestTextUtils(unittest.TestCase):

    def testLazy(self):
        r = blabla(3)
        self.assertEqual(r.value, None)
        v = str(r)
        self.assertEqual(v, 'AAAAAAAAAAAAAAAAAAAA 3')
        self.assertEqual(r.value, v)
