import random
import time
import base64
import hmac
import hashlib
import binascii
from copy import copy

from pulsar.apps.wsgi import Auth
from pulsar.apps.http import HttpClient
from pulsar.utils.httpurl import iri_to_uri
from pulsar import HTTPError

from .utils import (make_nonce, OAuthSignatureMethod_HMAC_SHA1, OAuthConsumer,
                    OAuthToken)


class KeyAuth(Auth):
    
    def __init__(self, **params):
        self.params = params
        
    def __call__(self, r):
        r.data.update(self.params)
        

class OAuthError(HTTPError):
    pass


class OAuth(Auth):
    '''Base class for OAuth Authentication.

.. attribute:: consumer

    The :class:`OAuthConsumer` for this :class:`OAuth`.

.. attribute:: access_token

    The access token obtained after the authorization process. This token
    is used to sign request to send to the server.

.. attribute:: authorised

    ``True`` if this `OAuth` is authorised.
'''
    authorization_url = '/oauth/authorize'
    access_token_url = '/oauth/token'
    signature_method = OAuthSignatureMethod_HMAC_SHA1()

    def __init__(self, id=None, secret=None, access_token=None,
                 http=None, **kwargs):
        self.access_token = access_token
        if http is None:
            http = HttpClient()
        self.http = http
        self.consumer = OAuthConsumer(id, secret, **kwargs)

    @property
    def version(self):
        return '0.0'

    @property
    def type(self):
        return 'OAuth %s' % self.version

    @property
    def authorised(self):
        return bool(self.access_token)

    def authorisation_parameters(self, rtoken, callback, **kwargs):
        return {}

    def access_token_parameters(self, callback=None):
        return {}

    def fetch_authentication_uri(self, **kwargs):
        parameters = self.authorisation_parameters(**kwargs)
        return iri_to_uri(self.authorization_url, parameters)

    def make_timestamp(self):
        """Get seconds since epoch (UTC)."""
        return str(int(time.time()))


class OAuth1(OAuth):
    '''A OAuth web-application Client for OAuth 1 Protocol (rfc5849).
This is a 3-legs authentication process:

 * The client obtains a set of temporary credentials from the server via
   the :attr:`request_token_url`
 * The temporary credentials are used to identify the access request located
   at the :attr:`authorization_url`. The resource owner authorizes the server
   to grant the client's access request.
 * The client uses the temporary credentials to request a set of
   token credentials from the server via the :attr:`access_token_url`.

Check http://tools.ietf.org/html/rfc5849 for details.'''
    request_token_url = '/oauth/request'

    @property
    def version(self):
        return '1.0'

    def request_token_parameters(self, callback=None):
        '''A dictionary of extra parameters to include in the
:attr:`request_token_url`.'''
        consumer = self.consumer
        return {'oauth_consumer_key': consumer.id,
                'oauth_timestamp': self.make_timestamp(),
                'oauth_nonce': self.make_nonce(),
                'oauth_callback': callback or '',
                'oauth_version': self.version}

    def authorisation_parameters(self, rtoken, callback, state=None,
                                 response_type=None):
        return {'oauth_token': rtoken.key,
                'oauth_callback': callback}

    def fetch_request_token(self, callback=None):
        """Return the request token. This is the first stage of the
authorisation process."""
        url = self.request_token_url
        method = self.REQUEST_METHOD
        parameters = self.request_token_parameters(callback=callback)
        parameters = self.signature_method.sign('GET', url, parameters,
                                                self.consumer)
        response = self.fetch_response(url, parameters, method)
        return response.add_callback(token_from_response)

    def access_token(self, key, secret, data):
        '''From a *data* dictionary in the redirected response from the
OAuth server, we build the last step of the authentication process. Request
the access token.'''
        oauth_token = data.get('oauth_token')
        oauth_verifier = data.get('oauth_verifier')
        if not oauth_token:
            raise OAuthError('Authorization token not available')
        token = OAuthToken(key, secret)
        if verifier:
            token.set_verifier(verifier)
        return token

    def make_nonce(self):
        return make_nonce()

    def __call__(self, request):
        pass

