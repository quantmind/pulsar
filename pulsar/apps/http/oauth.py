try:
    from oauthlib import oauth1, oauth2
except ImportError:     # pragma    nocover
    oauth1 = None
    oauth2 = None

from pulsar.api import ImproperlyConfigured
from pulsar.utils.structures import mapping_iterator

from . import auth


class OAuth1(auth.Auth):
    '''Add OAuth1 authentication to pulsar :class:`.HttpClient`
    '''
    available = bool(oauth1)

    def __init__(self, client_id=None, client=None, **kw):
        if oauth1 is None:  # pragma    nocover
            raise ImproperlyConfigured('%s requires oauthlib' %
                                       self.__class__.__name__)
        self._client = client or oauth1.Client(client_id, **kw)

    def __call__(self, response, **kw):
        r = response.request
        url, headers, _ = self._client.sign(
            r.url, r.method, r.body, r.headers)
        for key, value in mapping_iterator(headers):
            r.add_header(key, value)
        r.url = url


class OAuth2(auth.Auth):
    """Add OAuth2 authentication to pulsar :class:`.HttpClient`
    """
    available = bool(oauth2)

    def __init__(self, client_id=None, client=None, **kw):
        if oauth2 is None:  # pragma    nocover
            raise ImproperlyConfigured('%s requires oauthlib' %
                                       self.__class__.__name__)
        self.client = client or oauth2.WebApplicationClient(client_id, **kw)

    def __call__(self, response, **kw):
        r = response.request
        url, headers, _ = self.client.add_token(
            r.url, http_method=r.method, body=r.body, headers=r.headers)
        assert r.headers == headers
        # for key, value in mapping_iterator(headers):
        #     r.add_header(key, value)
        r.url = url
