import unittest

from pulsar.apps.http import HttpClient, HttpRequest, parse_qsl, urlparse


class TestClientCornerCases(unittest.TestCase):

    def test_headers(self):
        headers = HttpClient.DEFAULT_HTTP_HEADERS
        self.assertEqual(len(headers), 3)
        accept = headers['accept-encoding']
        self.assertTrue('gzip' in accept)
        self.assertTrue('deflate' in accept)

    def test_override_headers(self):
        headers = {'Accept': 'application/json, text/plain; q=0.8',
                   'content-type': 'application/json'}
        client = HttpClient(headers=headers)
        self.assertEqual(client.headers['accept'],
                         'application/json, text/plain; q=0.8')
        self.assertEqual(client.headers['content-type'], 'application/json')

    def test_urlparams(self):
        http = HttpClient()
        params = {'page': 2, 'key': 'foo'}
        request = HttpRequest(http, 'http://bla.com?k=6', 'post',
                              params=params)
        url = urlparse(request.url)
        data = parse_qsl(url.query)
        self.assertEqual(len(data), 3)
