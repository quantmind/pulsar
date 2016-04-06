import unittest

from pulsar import HttpRedirect


class WsgiRequestTests(unittest.TestCase):

    def test_httpredirect(self):
        location = 'https://pythonhosted.org/pulsar'
        r = HttpRedirect(location)
        self.assertEqual(r.location, location)
        r = HttpRedirect(location, headers=[('Content-Type', 'text/html')])
        self.assertEqual(len(r.headers), 2)
        self.assertEqual(r.location, location)
