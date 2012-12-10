from pulsar.apps.test import unittest

from .pshell import PulsarShell
        

class TestShell(unittest.TestCase):
    
    def testApp(self):
        app = PulsarShell()
        self.assertEqual(app.name, 'pulsarshell')
        self.assertEqual(app.callable, None)
        self.assertTrue(app.can_kill_arbiter)