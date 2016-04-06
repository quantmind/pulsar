'''Tests git info.'''
import unittest

from pulsar.utils.version import gitrepo


class TestGit(unittest.TestCase):

    def test_pulsar(self):
        info = gitrepo()
        self.assertTrue(info)
        self.assertTrue(info['branch'])
        self.assertIsInstance(info['head'], dict)
        self.assertIsInstance(info['remotes'], list)
        remote = info['remotes'][0]
        self.assertIsInstance(remote, dict)
        self.assertEqual(remote['name'], 'origin')
