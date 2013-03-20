'''API design'''
import sys
from pulsar.apps.test import unittest
from pulsar.utils.pep import get_event_loop
from pulsar import multi_async



class TestApi(unittest.TestCase):
    
    def test_empy_list(self):
        r = multi_async(())
        self.assertTrue(r.called)
        self.assertEqual(r.result, [])
        
    def test_empy_dict(self):
        r = multi_async({})
        self.assertTrue(r.called)
        self.assertEqual(r.result, {})