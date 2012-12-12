import pulsar
from pulsar.apps import fire_event
from pulsar.apps.test import unittest
    
    
class TestApp(unittest.TestCase):
    
    def test_fire_event(self):
        def blabla():
            pass
        self.assertRaises(ValueError, fire_event, blabla)