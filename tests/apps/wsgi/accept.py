from pulsar.apps.wsgi import Accept
from pulsar.apps.test import unittest


class AcceptTests(unittest.TestCase):
    
    def test_empty_mime(self):
        environ = {}
        a = Accept(environ)
        mime = a.mimetypes
        self.assertFalse(mime)
        
    def test_mime(self):
        environ = {'HTTP_ACCEPT': 'text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8'}
        a = Accept(environ)
        mime = a.mimetypes
        self.assertTrue(mime)
        self.assertEqual(len(mime), 4)
        self.assertEqual(id(mime), id(a.mimetypes))
        self.assertTrue('text/html' in mime)
        self.assertTrue('text/plain' in mime)
        self.assertEqual(mime.quality('text/html'), 1)
        self.assertEqual(mime.quality('text/plain'), 0.8)
        self.assertEqual(mime.quality('application/json'), 0.8)
        self.assertEqual(mime.best, 'text/html')
        
    def test_best(self):
        environ = {'HTTP_ACCEPT': 'text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8'}
        a = Accept(environ)
        mime = a.mimetypes
        self.assertTrue(mime)
        self.assertEqual(mime.best_match(('text/html', 'application/json')),
                         'text/html')
        self.assertEqual(mime.best_match(('application/json', 'text/html')),
                         'text/html')