import unittest

from pulsar.apps.wsgi import WsgiRequest


class AcceptTests(unittest.TestCase):

    def test_empty_mime(self):
        environ = {}
        a = WsgiRequest(environ)
        content_types = a.content_types
        self.assertFalse(content_types)

    def test_content_types(self):
        environ = {'HTTP_ACCEPT': ('text/html, application/xhtml+xml, '
                                   'application/xml;q=0.9, */*;q=0.8')}
        a = WsgiRequest(environ)
        content_types = a.content_types
        self.assertTrue(content_types)
        self.assertEqual(len(content_types), 4)
        self.assertEqual(id(content_types), id(a.content_types))
        self.assertTrue('text/html' in content_types)
        self.assertTrue('text/plain' in content_types)
        self.assertEqual(content_types.quality('text/html'), 1)
        self.assertEqual(content_types.quality('text/plain'), 0.8)
        self.assertEqual(content_types.quality('application/json'), 0.8)
        self.assertEqual(content_types.best, 'text/html')

    def test_best(self):
        environ = {'HTTP_ACCEPT': ('text/html, application/xhtml+xml, '
                                   'application/xml;q=0.9, */*;q=0.8')}
        a = WsgiRequest(environ)
        content_types = a.content_types
        self.assertTrue(content_types)
        self.assertEqual(content_types.best_match(('text/html',
                                                   'application/json')),
                         'text/html')
        self.assertEqual(content_types.best_match(('application/json',
                                                   'text/html')),
                         'text/html')
