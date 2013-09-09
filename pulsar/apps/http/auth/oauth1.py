import random
import time
import base64
import hmac
import hashlib
import binascii
from copy import copy

from oauthlib.common import add_params_to_uri, urldecode
from oauthlib import oauth1

from pulsar.utils.httpurl import iri_to_uri, make_nonce, parse_qs
from pulsar import HTTPError

from .basic import Auth
        

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

    def __init__(self, client_key,
                 client_secret=None,
                 resource_owner_key=None,
                 resource_owner_secret=None,
                 callback_uri=None,
                 signature_method=oauth1.SIGNATURE_HMAC,
                 signature_type=oauth1.SIGNATURE_TYPE_AUTH_HEADER,
                 rsa_key=None,
                 verifier=None):
        self._client = oauth1.Client(client_key,
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

    
class OAuthToken(object):
    """OAuthToken is a data type that represents an End User via either an access
    or request token.
    
    key -- the token
    secret -- the token secret

    """
    callback = None
    callback_confirmed = None
    verifier = None
    
    @classmethod
    def from_string(cls, text):
        params = parse_qs(response, keep_blank_values=False)
        key = params['oauth_token'][0]
        secret = params['oauth_token_secret'][0]
        token = cls(key, secret)
        try:
            token.callback_confirmed = params['oauth_callback_confirmed'][0]
        except KeyError:
            pass # 1.0, no callback confirmed.
        url = params.get('xoauth_request_auth_url',None)
        if url:
            token.set_callback(url)
        return token

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    def set_callback(self, callback):
        self.callback = callback
        self.callback_confirmed = 'true'

    def set_verifier(self, verifier=None):
        if verifier is not None:
            self.verifier = verifier
        else:
            self.verifier = generate_verifier()

    def get_callback_url(self):
        if self.callback and self.verifier:
            # Append the oauth_verifier.
            parts = urlparse(self.callback)
            scheme, netloc, path, params, query, fragment = parts[:6]
            if query:
                query = '%s&oauth_verifier=%s' % (query, self.verifier)
            else:
                query = 'oauth_verifier=%s' % self.verifier
            return urlunparse((scheme, netloc, path, params,
                query, fragment))
        return self.callback

    def to_string(self):
        data = {
            'oauth_token': self.key,
            'oauth_token_secret': self.secret,
        }
        if self.callback_confirmed is not None:
            data['oauth_callback_confirmed'] = self.callback_confirmed
        return urllib.urlencode(data)
 
    def __str__(self):
        return self.to_string()
    
    
class OAuthSignatureMethod(object):
    """A strategy class that implements a signature method."""
    name=  ''
    def __str__(self):
        return self.name
    __repr__ = __str__

    def sign(self, method, uri, parameters, consumer, token=None):
        parameters = copy(parameters)
        raw = self.base_string(method, uri, parameters)
        signature = self.build_signature(raw, consumer, token)
        parameters['oauth_signature'] = native_str(signature)
        return parameters
    
    def base_string(self, method, uri, parameters):
        parameters['oauth_signature_method'] = str(self)
        p = dict(((escape(k),escape(parameters[k])) for k in parameters))
        params = '&'.join((k+'='+p[k] for k in sorted(p)))
        return '&'.join((escape(method),escape(uri),escape(params)))
    
    def build_signature(self, raw, consumer, token):        
        raise NotImplementedError()


class OAuthSignatureMethod_HMAC_SHA1(OAuthSignatureMethod):
    name = 'HMAC-SHA1'
        
    def build_signature(self, raw, consumer, token):
        key = escape(consumer.secret) + '&'
        if token:
            key += escape(token.secret)
        hashed = hmac.new(to_bytes(key), to_bytes(raw), hashlib.sha1)
        return binascii.b2a_base64(hashed.digest())[:-1]


class OAuthSignatureMethod_PLAINTEXT(OAuthSignatureMethod):
    name = 'PLAINTEXT'

    def build_signature(self, raw, consumer, token):
        """Concatenates the consumer key and secret."""
        key = consumer.secret + '&'
        if token:
            key += token.secret
        return urlquote(key)
    
    
################################################################################
##  REQUEST/RESPONSE TOOLS
################################################################################
def token_from_response(response):
    text = response.content_string()
    return OAuthToken.from_string(text)

