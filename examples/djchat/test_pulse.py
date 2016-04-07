"""Tests the pulse Command."""
import unittest

from pulsar.apps import wsgi
try:
    from pulsar.apps.pulse.management.commands.pulse import Command
except ImportError:
    Command = None


@unittest.skipUnless(Command, 'Requires django')
class pulseCommandTest(unittest.TestCase):

    def test_pulse(self):
        cmnd = Command()
        hnd = cmnd.handle(dryrun=True)
        self.assertTrue(isinstance(hnd, wsgi.LazyWsgi))
