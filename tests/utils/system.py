'''Tests the tools and utilities in pulsar.utils.'''
import unittest

from pulsar import system, platform


class TestSystem(unittest.TestCase):

    @unittest.skipUnless(platform.is_posix, 'Posix platform required')
    def testPlatform(self):
        self.assertFalse(platform.is_windows)
        self.assertFalse(platform.is_winNT)

    def test_maxfd(self):
        m = system.get_maxfd()
        self.assertTrue(m)
