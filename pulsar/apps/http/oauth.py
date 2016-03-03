try:
    from oauthlib import oauth1, oauth2
    from oauthlib.common import generate_token
except ImportError:
    oauth1 = None
    oauth2 = None

from pulsar import ImproperlyConfigured
from pulsar.utils.structures import mapping_iterator

from . import auth


class OAuth1(auth.Auth):
    '''Add OAuth1 authentication to pulsar :class:`.HttpClient`
    '''

    def __init__(self, client_id=None, client=None, **kw):
        if oauth1 is None:
            raise ImproperlyConfigured('%s requires oauthlib' %
                                       self.__class__.__name__)
        self._client = client or oauth1.Client(client_id, **kw)

    def __call__(self, response, exc=None):
        r = response.request
        url, headers, data = self._client.sign(r.full_url, r.method, r.data,
                                               r.headers)
        for key, value in mapping_iterator(headers):
            r.add_header(key, value)
        r.full_url = url
        r.data = data


class OAuth2(auth.Auth):
    '''Add OAuth2 authentication to pulsar :class:`.HttpClient`'''

    def __init__(self, client_id=None, client=None, **kw):
        if oauth2 is None:
            raise ImproperlyConfigured('%s requires oauthlib' %
                                       self.__class__.__name__)
        self.client = client or oauth2.WebApplicationClient(client_id, **kw)

    def __call__(self, response, exc=None):
        r = response.request
        url, headers, data = self.client.add_token(
            r.full_url, http_method=r.method, body=r.data, headers=r.headers)
        for key, value in mapping_iterator(headers):
            r.add_header(key, value)
        r.full_url = url
        r.data = data

    def prepare_request_uri(self, url, state=None, **kwargs):
        """Prepare the request uri and return a tuple with url and state

        :param url: base url for OAuth2 requests
        :param state: optional state, if not supplied one is generated
        :param kwargs: additinal paparameters
        :return: a tuple with url and state
        """
        state = state or generate_token()
        url = self.client.prepare_request_uri(url, state=state, **kwargs)
        return url, state
