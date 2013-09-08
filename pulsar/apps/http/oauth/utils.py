from cgi import parse_qs


def make_nonce():
    """Generate pseudorandom number."""
    return str(random.randint(0, 100000000))


class OAuthConsumer(object):
    def __init__(self, id, secret, scope=None):
        self.id = id
        self.secret = secret
        self.scope = scope
        
    def is_valid(self):
        return self.id and self.secret
    
    def __str__(self):
        return self.id
    __repr__ = __str__
    
    
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

