import json

from pulsar import Deferred
from pulsar.apps import wsgi
from pulsar.apps.test import unittest


class TestAsyncContent(unittest.TestCase):
    
    def test_simple_json(self):
        response = wsgi.Json({'bla': 'foo'})
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type, 'application/json')
        self.assertFalse(response.as_list)
        self.assertEqual(response.render(), json.dumps({'bla': 'foo'}))
        
    def test_simple_json_as_list(self):
        response = wsgi.Json({'bla': 'foo'}, as_list=True)
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type, 'application/json')
        self.assertTrue(response.as_list)
        self.assertEqual(response.render(), json.dumps([{'bla': 'foo'}]))
        
    def test_json_with_async_string(self):
        astr = wsgi.AsyncString('ciao')
        response = wsgi.Json({'bla': astr})
        self.assertEqual(len(response.children), 1)
        self.assertEqual(response.content_type, 'application/json')
        self.assertEqual(response.render(), json.dumps({'bla': 'ciao'}))
        
    def test_json_with_async_string2(self):
        d = Deferred()
        astr = wsgi.AsyncString(d)
        response = wsgi.Json({'bla': astr})
        self.assertEqual(len(response.children), 1)
        result = response.render()
        self.assertIsInstance(result, Deferred)
        d.callback('ciao')
        result = yield result
        self.assertEqual(result, json.dumps({'bla': 'ciao'}))
        