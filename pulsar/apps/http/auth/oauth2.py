from oauthlib import oauth2

from .oauth1 import OAuth


class OAuth2(OAuth):
    '''Construct a new OAuth 2 authorisation object.

        :param client_id: Client id obtained during registration
        :param client: :class:`oauthlib.oauth2.Client` to be used. Default is
                       WebApplicationClient which is useful for any
                       hosted application but not mobile or desktop.
        :param token: Token dictionary, must include access_token
                      and token_type.
    '''
    def __init__(self, client_id=None, client=None, token=None):
        self._client = client or oauth2.WebApplicationClient(client_id,
                                                             token=token)
        if token:
            for k, v in token.items():
                setattr(self._client, k, v)

    def __call__(self, response):
        request = response.request
        url, headers, body = self._client.add_token(request.full_url,
            http_method=request.method, body=request.data,
            headers=request.headers)
        return response

