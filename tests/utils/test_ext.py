import os
import unittest

from pulsar import api


@unittest.skipIf(os.environ.get('PULSARPY') == 'yes', 'Pulsar extensions')
class TestApi(unittest.TestCase):

    def test_has(self):
        self.assertTrue(api.HAS_C_EXTENSIONS)
