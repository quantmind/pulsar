'''Tests the tools and utilities in pulsar.utils.'''
import unittest

from pulsar.utils.system import platform, get_maxfd


class TestSystem(unittest.TestCase):

    @unittest.skipUnless(platform.is_posix, 'Posix platform required')
    def testPlatform(self):
        self.assertFalse(platform.is_windows)

    def test_maxfd(self):
        m = get_maxfd()
        self.assertTrue(m)
