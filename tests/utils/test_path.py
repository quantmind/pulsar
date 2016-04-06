import unittest

from pulsar.utils.path import Path


class TestPath(unittest.TestCase):

    def testThis(self):
        p = Path(__file__)
        self.assertTrue(p.isfile())
        self.assertFalse(p.isdir())
        c = Path.cwd()
        self.assertNotEqual(p, c)
        self.assertTrue(c.isdir())

    def testDir(self):
        c = Path.cwd()
        self.assertEqual(c, c.dir())
        c = Path('/sdjc/scdskjcdnsd/dhjdhjdjksdjksdksd')
        self.assertFalse(c.exists())
        self.assertRaises(ValueError, c.dir)

    def testAdd2Python(self):
        p = Path('/sdjc/scdskjcdnsd/dhjdhjdjksdjksdksd')
        module = p.add2python('pulsar')
        self.assertEqual(module.__name__, 'pulsar')
        self.assertRaises(ValueError, p.add2python, 'kaputttt')

    def testAdd2Python_failure(self):
        p = Path()
        self.assertRaises(ImportError, p.add2python, 'kaputttt')
        self.assertFalse(p.add2python('vdfvdavfdv', must_exist=False))
