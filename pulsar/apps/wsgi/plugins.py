'''Utility classes for managing Header Authentication, used both by
servers and clients.

header parser
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_authorization_header


Auth
~~~~~~~~~~~~~~~~~

.. autoclass:: Auth
   :members:
   :member-order: bysource
   
   
HTTPBasicAuth
~~~~~~~~~~~~~~~~~

.. autoclass:: HTTPBasicAuth
   :members:
   :member-order: bysource
   
HTTPDigestAuth
~~~~~~~~~~~~~~~~~

.. autoclass:: HTTPDigestAuth
   :members:
   :member-order: bysource
   
'''
import os
import time
from hashlib import sha1
from base64 import b64encode, b64decode

from pulsar.utils.httpurl import parse_dict_header, hexmd5, hexsha1, urlparse
from pulsar.utils.pep import native_str

__all__ = ['Auth', 
           'HTTPBasicAuth',
           'HTTPDigestAuth',
           'basic_auth_str',
           'parse_authorization_header']
        

class Auth(object):
    """Base class for managing HTTP header authentication"""
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
            raise ValueError('Unknown algorithm %s' % self.algorithm)
        
    def ha1(self, realm, password):
        return self.hex('%s:%s:%s' % (self.username, realm, password))
    
    def ha2(self, qop, method, uri, body=None):
        if qop == "auth" or qop is None:
            return self.hex("%s:%s" % (method, uri))
        elif qop == "auth-int":
            return self.hex("%s:%s:%s" % (method, uri, self.hex(body)))
        raise ValueError()


def basic_auth_str(username, password):
    """Returns a Basic Auth string."""
    b64 = b64encode(('%s:%s' % (username, password)).encode('latin1'))
    return 'Basic %s' % native_str(b64.strip(), 'latin1')

digest_parameters=frozenset(('username', 'realm', 'nonce', 'uri', 'nc',
                             'cnonce',  'response'))
def parse_authorization_header(value, charset='utf-8'):
    """Parse an HTTP basic/digest authorization header transmitted by the web
browser.  The return value is either `None` if the header was invalid or
not given, otherwise an :class:`Auth` object.

:param value: the authorization header to parse.
:return: a :class:`Auth` or `None`."""
    if not value:
        return
    try:
        auth_type, auth_info = value.split(None, 1)
        auth_type = auth_type.lower()
    except ValueError:
        return
    if auth_type == 'basic':
        try:
            up = b64decode(auth_info.encode('latin-1')).decode(charset)
            username, password = up.split(':', 1)
        except Exception:
            return
        return HTTPBasicAuth(username, password)
    elif auth_type == 'digest':
        auth_map = parse_dict_header(auth_info)
        if not digest_parameters.difference(auth_map):
            return HTTPDigestAuth(auth_map.pop('username'),
                                  options=auth_map)
