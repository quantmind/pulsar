'''Tests the pulse Command.'''
import unittest

try:
    from pulsar.apps.pulse.management.commands.pulse import Command
except ImportError:
    Command = None
from pulsar.apps import wsgi


@unittest.skipUnless(Command, 'Requires django')
class pulseCommandTest(unittest.TestCase):

    def test_pulse(self):
        cmnd = Command()
        hnd = cmnd.handle(dryrun=True)
        self.assertTrue(isinstance(hnd, wsgi.LazyWsgi))
