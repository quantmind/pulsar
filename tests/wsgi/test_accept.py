import unittest

from pulsar.apps.test import test_wsgi_request


class AcceptTests(unittest.TestCase):

    async def test_empty_mime(self):
        request = await test_wsgi_request(headers=[('Accept', None)])
        content_types = request.content_types
        self.assertFalse(content_types)

    async def test_content_types(self):
        request = await test_wsgi_request(headers=[
            ('Accept', 'text/html, application/xhtml+xml, '
                       'application/xml;q=0.9, */*;q=0.8')])
        content_types = request.content_types
        self.assertTrue(content_types)
        self.assertEqual(len(content_types), 4)
        self.assertEqual(id(content_types), id(request.content_types))
        self.assertTrue('text/html' in content_types)
        self.assertTrue('text/plain' in content_types)
        self.assertEqual(content_types.quality('text/html'), 1)
        self.assertEqual(content_types.quality('text/plain'), 0.8)
        self.assertEqual(content_types.quality('application/json'), 0.8)
        self.assertEqual(content_types.best, 'text/html')

    async def test_best(self):
        request = await test_wsgi_request(headers=[
            ('Accept', 'text/html, application/xhtml+xml, '
                       'application/xml;q=0.9, */*;q=0.8')])
        content_types = request.content_types
        self.assertTrue(content_types)
        self.assertEqual(content_types.best_match(('text/html',
                                                   'application/json')),
                         'text/html')
        self.assertEqual(content_types.best_match(('application/json',
                                                   'text/html')),
                         'text/html')
