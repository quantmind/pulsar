'''Tests the pulse Command.'''
try:
    from examples.djangoapp.djpulsar.management.commands.pulse import Command
except ImportError:
    Command = None
from pulsar.apps import wsgi
from pulsar.apps.test import unittest


@unittest.skipUnless(Command, 'Requires django')
class pulseCommandTest(unittest.TestCase):
    
    def test_pulse(self):
        cmnd = Command()
        hnd = cmnd.handle(dryrun=True)
        self.assertTrue(isinstance(hnd, wsgi.LazyWsgi))