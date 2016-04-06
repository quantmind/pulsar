'''Test cases for code not covered in standard test cases'''
import tempfile
import shutil
import unittest


class PulsarCoverage(unittest.TestCase):

    def test_profile_plugin(self):
        from pulsar.apps.test.plugins import profile
        p = profile.Profile()
        p.config.set('profile', True)
        p.configure(p.config)
        p.profile_temp_path = tempfile.mkdtemp()
        self.assertFalse(p.on_end())
        shutil.rmtree(p.profile_temp_path)
        #
        lines = list(profile.data_stream(['', 'a b c d e f']))
        self.assertEqual(lines, [])
