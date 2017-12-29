import os
import unittest

from pulsar import api
from pulsar.utils.system import platform


@unittest.skipIf(os.environ.get('PULSARPY') == 'yes', 'Pulsar extensions')
class TestApi(unittest.TestCase):

    def test_has(self):
        try:
            self.assertTrue(api.HAS_C_EXTENSIONS)
        except Exception:
            if not platform.is_windows:
                raise
