from pulsar.utils.httpurl import basic_auth_str

__all__ = ['Auth', 'HTTPBasicAuth', 'HTTPDigestAuth']


class Auth(object):
    """Base class that all auth implementations derive from"""
    type = None
    def __call__(self, r):
        raise NotImplementedError('Auth hooks must be callable.')

    def authenticated(self, *args, **kwargs):
        return False

    def __str__(self):
        return self.__repr__()


class HTTPBasicAuth(Auth):
    """Attaches HTTP Basic Authentication to the given Request object."""
    def __init__(self, username, password):
        self.username = username
        self.password = password
        
    @property
    def type(self):
        return 'basic'

    def __call__(self, request):
        request.headers['Authorization'] = basic_auth_str(self.username,
                                                          self.password)

    def authenticated(self, environ, username, password):
        return username==self.username and password==self.password

    def __repr__(self):
        return 'Basic: %s' % self.username


class HTTPDigestAuth(Auth):
    """Attaches HTTP Digest Authentication to the given Request object."""
    def __init__(self, username, password=None, options=None):
        self.username = username
        self.password = password
        self.last_nonce = None
        self.options = options or {}
        self.algorithm = self.options.pop('algorithm', 'MD5')

    @property
    def type(self):
        return 'digest'
    
    def __call__(self, request):
        # If we have a saved nonce, skip the 401
        if self.last_nonce:
            request.headers['Authorization'] =\
                self.encode(request.method, request.full_url)
        request.register_hook('post_request', self.handle_401)
        return request

    def __repr__(self):
        return 'Digest: %s' % self.username

    def authenticated(self, environ, username, password):
        '''Called by the server to check if client is authenticated.'''
        if username != self.username:
            return False
        o = self.options
        qop = o.get('qop')
        method = environ['REQUEST_METHOD']
        uri = environ.get('PATH_INFO','')
        ha1 = self.ha1(o['realm'], password)
        ha2 = self.ha2(qop, method, uri)
        if qop is None:
            response = hexmd5(":".join((ha1, self.nonce, ha2)))
        elif qop == 'auth' or qop == 'auth-int':
            response = hexmd5(":".join((ha1, o['nonce'], o['nc'],
                                        o['cnonce'], qop, ha2)))
        else:
            raise ValueError("qop value are wrong")
        return o['response'] == response

    def encode(self, method, uri):
        '''Called by the client to encode Authentication header.'''
        if not self.username or not self.password:
            return
        o = self.options
        qop = o.get('qop')
        realm = o.get('realm')
        nonce = o['nonce']
        entdig = None
        p_parsed = urlparse(uri)
        path = p_parsed.path
        if p_parsed.query:
            path += '?' + p_parsed.query
        KD = lambda s, d: self.hex("%s:%s" % (s, d))
        ha1 = self.ha1(realm, self.password)
        ha2 = self.ha2(qop, method, path)
        if qop == 'auth':
            if nonce == self.last_nonce:
                self.nonce_count += 1
            else:
                self.nonce_count = 1
            ncvalue = '%08x' % self.nonce_count
            s = str(self.nonce_count).encode('utf-8')
            s += nonce.encode('utf-8')
            s += time.ctime().encode('utf-8')
            s += os.urandom(8)
            cnonce = sha1(s).hexdigest()[:16]
            noncebit = "%s:%s:%s:%s:%s" % (nonce, ncvalue, cnonce, qop, ha2)
            respdig = KD(ha1, noncebit)
        elif qop is None:
            respdig = KD(ha1, "%s:%s" % (nonce, ha2))
        else:
            # XXX handle auth-int.
            return
        base = 'username="%s", realm="%s", nonce="%s", uri="%s", ' \
               'response="%s"' % (self.username, realm, nonce, path, respdig)
        opaque = o.get('opaque')
        if opaque:
            base += ', opaque="%s"' % opaque
        if entdig:
            base += ', digest="%s"' % entdig
            base += ', algorithm="%s"' % self.algorithm
        if qop:
            base += ', qop=%s, nc=%s, cnonce="%s"' % (qop, ncvalue, cnonce)
        return 'Digest %s' % (base)
        
    def handle_401(self, response):
        """Takes the given response and tries digest-auth, if needed."""
        request = response.request
        num_401_calls = request.hooks['response'].count(self.handle_401)
        s_auth = response.headers.get('www-authenticate', '')
        if 'digest' in s_auth.lower() and num_401_calls < 2:
            self.options = parse_dict_header(s_auth.replace('Digest ', ''))
            request.headers['Authorization'] = self.encode(request.method,
                                                           request.full_url)
            response.again = True
        return response
    
    def hex(self, x):
        if self.algorithm == 'MD5':
            return hexmd5(x)
        elif self.algorithm == 'SHA1':
            return hexsha1(x)
        else:
            raise valueError('Unknown algorithm %s' % self.algorithm)
        
    def ha1(self, realm, password):
        return self.hex('%s:%s:%s' % (self.username, realm, password))
    
    def ha2(self, qop, method, uri, body=None):
        if qop == "auth" or qop is None:
            return self.hex("%s:%s" % (method, uri))
        elif qop == "auth-int":
            return self.hex("%s:%s:%s" % (method, uri, self.hex(body)))
        raise ValueError()
