'''Tests the tools and utilities in pulsar.utils.'''
from pulsar import system, platform
from pulsar.apps.test import unittest


class TestSystem(unittest.TestCase):
    
    @unittest.skipUnless(platform.is_posix, 'Posix platform required')
    def testPlatform(self):
        self.assertFalse(platform.is_vista)
        self.assertFalse(platform.is_windows)
        self.assertFalse(platform.is_winNT)
        
    def test_maxfd(self):
        m = system.get_maxfd()
        self.assertTrue(m)