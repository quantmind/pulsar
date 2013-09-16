from pulsar import EventHandler, Failure
from pulsar.apps.test import unittest 


class TestFailure(unittest.TestCase):
    
    def test_one_time(self):
        h = EventHandler(one_time_events=('start', 'finish'))
        self.assertEqual(h.event('finish').name, 'finish')
        h.bind_event('finish', lambda f: 'OK')
        result = h.fire_event('finish')
        self.assertTrue(h.event('finish').done())
        self.assertEqual(result, 'OK')
        
    def test_one_time_error(self):
        h = EventHandler(one_time_events=('start', 'finish'))
        self.assertEqual(h.event('finish').name, 'finish')
        h.bind_event('finish', lambda f: 'OK'+4)
        result = h.fire_event('finish')
        self.assertTrue(h.event('finish').done())
        self.assertTrue(isinstance(result, Failure))
        result.mute()