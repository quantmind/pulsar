'''OAuth version 1 & 2'''
from .basic import *
try:
    import oauthlib
    from .oauth1 import OAuth, OAuth1, OAuthError
    from .oauth2 import OAuth2
except ImportError:
    oathlib = None
    OAuth1 = None
    OAuth2 = None