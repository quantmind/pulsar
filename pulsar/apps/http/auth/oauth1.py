import time

from oauthlib.common import add_params_to_uri, urldecode
from oauthlib import oauth1

from pulsar import HTTPError

from .basic import Auth


class OAuthError(HTTPError):
    pass


class OAuth1(Auth):
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

    def __init__(self,
                 client_key,
                 client_secret=None,
                 resource_owner_key=None,
                 resource_owner_secret=None,
                 callback_uri=None,
                 signature_method=oauth1.SIGNATURE_HMAC,
                 signature_type=oauth1.SIGNATURE_TYPE_AUTH_HEADER,
                 rsa_key=None,
                 verifier=None):
        self._client = oauth1.Client(
            client_key,
            client_secret=client_secret,
            resource_owner_key=resource_owner_key,
            resource_owner_secret=resource_owner_secret,
            callback_uri=callback_uri,
            signature_method=signature_method,
            signature_type=signature_type,
            rsa_key=rsa_key,
            verifier=verifier)

    @property
    def version(self):
        return '1.0'

    def request_token_parameters(self, callback=None):
        '''A dictionary of extra parameters to include in the
:attr:`request_token_url`.'''
        consumer = self.consumer
        timestamp = int(time.time())
        return {'oauth_consumer_key': consumer.id,
                'oauth_timestamp': str(timestamp),
                'oauth_nonce': make_nonce(timestamp),
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

    def __call__(self, request):
        pass
