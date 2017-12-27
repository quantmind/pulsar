import unittest
from urllib.parse import parse_qsl, urlparse

from pulsar.apps.test import allowFailure
from pulsar.apps.http import (
    HttpClient, HttpRequest, OAuth1, OAuth2
)


class TestClientCornerCases(unittest.TestCase):

    def test_headers(self):
        http = HttpClient()
        self.assertEqual(len(http.headers), 5)
        accept = http.headers.getall('accept-encoding')
        self.assertEqual(accept, ['deflate', 'gzip'])

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

    def test_empty_params(self):
        http = HttpClient()
        request = HttpRequest(http, 'http://bla.com?k', 'get')
        self.assertEqual(request.url, 'http://bla.com?k=')

    @unittest.skipUnless(OAuth1.available, 'oauthlib not available')
    async def test_oauth1(self):
        oauth = OAuth1(
            'random',
            client_secret='xxxxxxx',
            resource_owner_key='xxxxxxx',
            resource_owner_secret='xxxxxxx'
        )
        async with HttpClient() as http:
            await http.post(
                'https://api.github.com/gists/public',
                pre_request=oauth
            )

    @unittest.skipUnless(OAuth2.available, 'oauthlib not available')
    async def test_oauth2(self):
        oauth = OAuth2(
            'random',
            client_secret='xxxxxxx',
            resource_owner_key='xxxxxxx',
            resource_owner_secret='xxxxxxx',
            access_token='xxxxxx'
        )
        async with HttpClient() as http:
            await http.post(
                'https://api.github.com/gists/public',
                pre_request=oauth
            )

    @allowFailure
    async def test_redirect_session(self):
        http = HttpClient()
        resp = await http.request(
            'GET',
            'https://selftrade.co.uk/transactional/anonymous/login'
        )
        self.assertEqual(resp.status_code, 200)
