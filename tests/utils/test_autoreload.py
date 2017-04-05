import unittest
from unittest import skipUnless

from pulsar.utils import autoreload


class AutoReloadTest(unittest.TestCase):

    @skipUnless('watchdog' in autoreload.reloaders, 'Requires watchdog')
    def test_watchdog(self):
        self.assertEqual(autoreload.reloaders['auto'],
                         autoreload.reloaders['watchdog'])
        reloader = autoreload.reloaders['auto']()
        self.assertEqual(reloader.interval, 1)
