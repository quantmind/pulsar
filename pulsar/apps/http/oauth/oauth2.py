from pulsar.utils.httpurl import HttpRedirect, urlencode, to_bytes

from .oauth1 import OAuth
from .utils import parse_qs


class OAuth2(OAuth):
    '''OAuth version 2. This is a two legs authorisation process with the
aim to obtain an access token used to sign requests.

* The client requests authorization from the resource owner. The client
  receives an authorization grant which is a credential representing the
  resource owner's authorization.
* The client requests an access token by authenticating with the
  authorization server and presenting the authorization grant.

Check oauth2_ for more information.

.. specification: http://tools.ietf.org/html/draft-ietf-oauth-v2
.. oauth2: http://oauth.net/'''
    default_scope = ''

    @property
    def version(self):
        return '2.0'

    def autheticate(self, callback_url=None, **kwargs):
        url = self.fetch_authentication_uri(callback_url=callback_url, **kwargs)
        raise HttpRedirect(url)
        
    def authorisation_parameters(self, callback_url=None, state=None,
                                 response_type=None):
        '''Parameters used to construct the URI for the authorization request.
Check *Authorization Request* section 4.1.1'''
        client = self.consumer
        p = {'client_id': client.id,
             'response_type': response_type or 'code',
             'state': state or ''}
        p['scope'] = client.scope or self.default_scope
        if callback_url:
            p['redirect_uri'] = callback_url
        return p

    def authorisation_response(self, data, state=None):
        if state:
            if data.get('state') != state:
                raise OAuthError('state parameter in authorisation response'
                                 ' does not match request')
        if 'code' in data:
            code = data['code']
            data = {'code': data['code'],
                    'client_id': self.consumer.id,
                    'client_secret': self.consumer.secret,
                    'state': state or ''}
            #data = to_bytes(urlencode(data))
            response = self.http.post(self.access_token_url, data=data)
            if response.status_code == 200:
                self.access_token = response.content_json()['access_token']
            else:
                response.raise_for_status()

    def access_token_from_response(self, response):
        params = parse_qs(response.content_string, keep_blank_values=False)
        if 'access_token' in params:
            self.access_token = params['access_token'][0]
        else:
            raise OAuthError('Access token not provided by server')

    def __call__(self, request):
        if self.authorised:
            request.data['access_token'] = self.access_token

