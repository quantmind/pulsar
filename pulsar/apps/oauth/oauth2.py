import cgi

from .oauth1 import OAuthBase


class OAuth2(OAuth):
    '''OAuth version 2. This is a two legs authorisation process with the
aim to obtain an access token used to sign requests.

* The client requests authorization from the resource owner. The client
  receives an authorization grant which is a credential representing the
  resource owner's authorization.
* The client requests an access token by authenticating with the
  authorization server and presenting the authorization grant.
http://tools.ietf.org/html/draft-ietf-oauth-v2-28'''
    default_scope = ''
    
    @property
    def version(self):
        return '2.0'
    
    def authorisation_parameters(self, callback_url, state=None,
                                 response_type=None):
        '''Parameters used to construct the URI for the authorization request.
Check *Authorization Request* section 4.1.1'''
        client = self.consumer
        p = {'client_id': client.id,
             'response_type': response_type or 'code'}
        p['scope'] = client.scope or self.default_scope
        if callback:
            p['redirect_uri'] = callback
        if state:
            # This parameter is optional and can be used to prevent
            # cross-site request forgery
            p['state'] = state
        return p

    def authorisation_response(self, data, state=None):
        if state is not None:
            if data.get('state') != state:
                raise OAuthError('state parameter in authorisation response'
                                 ' does not match request')
        if 'code' in data:
            code = data['code']
            data = {'code': data['code'],
                    'clinet_id': self.consumer.id,
                    'client_secret': self.consumer.secret}
            response = http.post(self.access_token_url, data=data)
            return response.add_callback(self.access_token_from_response)
        
    def access_token_from_response(self, response):
        params = cgi.parse_qs(response.content_string, keep_blank_values=False)
        if 'access_token' in params:
            self.access_token = params['access_token'][0]
        else:
            raise OAuthError('Access token not provided by server')
    
    
class GitHub(OAuth2):
    authorization_url = 'https://github.com/login/oauth/authorize'
    access_token_url  = 'https://github.com/login/oauth/access_token'
    scopes = ('user','public_repo','repo','gist')
    
    def authorisation_parameters(self, *args, **kwargs):
        # override o that we can inject scope as comma separated rather than
        # space separated (Section 3.3)
        params = super(Github,self).authorisation_parameters(*args, **kwargs)
        params['scope'] = ','.join(params['scope'].split(' '))
        return params
    