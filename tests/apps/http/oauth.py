try:
    from pulsar.apps.http.auth import OAuth2
    from oauthlib import oauth2
except ImportError:
    OAuth2 = None
from pulsar.apps.test import unittest
    
from . import client


@unittest.skipUnless(OAuth2, 'Requires oauthlib')
class TestOAuth(client.TestHttpClientBase, unittest.TestCase):
    with_tls = True
    
    def client(self, **kw):
        client = super(TestOAuth, self).client(**kw)
        oauth2.Client('abc')
        client.bind_event('pre_request', OAuth2(client=oauth2.Client('abc')))
        return client
        
    def test_client(self):
        client = self.client()
        response = yield client.get(self.httpbin('oauth2/abc')).on_finished
        
    