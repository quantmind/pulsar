import os
import time
from hashlib import sha1
from base64 import b64encode
from urllib.parse import urlparse

from pulsar.utils.httpurl import (parse_dict_header, hexmd5, hexsha1,
                                  to_string, DEFAULT_CHARSET)

from .plugins import request_again, noerror


__all__ = ['Auth',
           'HTTPBasicAuth',
           'HTTPDigestAuth']


class Auth:
    '''Base class for managing authentication.
    '''
    type = None

    def __call__(self, response, exc=None):
        raise NotImplementedError

    def __str__(self):
        return self.__repr__()


class HTTPBasicAuth(Auth):
    '''HTTP Basic Authentication handler.'''
    def __init__(self, username, password, status_code=401):
        self.username = username
        self.password = password
        self.status_code = status_code

    @property
    def type(self):
        return 'basic'

    def __call__(self, response, exc=None):
        # pre_request event. Must return response instance!
        response.request.headers['Authorization'] = self.header()
        return response

    def header(self):
        b64 = b64encode(('%s:%s' % (
            self.username, self.password)).encode(DEFAULT_CHARSET))
        return 'Basic %s' % to_string(b64.strip(), DEFAULT_CHARSET)

    def __repr__(self):
        return 'Basic: %s' % self.username


class HTTPDigestAuth(Auth):
    '''HTTP Digest Authentication handler.'''
    def __init__(self, username, password=None, options=None):
        self.username = username
        self.password = password
        self.last_nonce = None
        self.options = options or {}
        self.algorithm = self.options.pop('algorithm', 'MD5')

    @property
    def type(self):
        return 'digest'

    def __call__(self, response, exc=None):
        # pre_request event. Must return response instance!
        # If we have a saved nonce, skip the 401
        if self.last_nonce:
            request = response.request
            request.headers['Authorization'] =\
                self.encode(request.method, request.full_url)
        else:
            # add post request handler
            response.bind_event('post_request', self.handle_401)
        return response

    def __repr__(self):
        return 'Digest: %s' % self.username

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
            respdig = self.KD(ha1, noncebit)
        elif qop is None:
            respdig = self.KD(ha1, "%s:%s" % (nonce, ha2))
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

    @noerror
    def handle_401(self, response, exc=None):
        """Takes the given response and tries digest-auth, if needed."""
        if response.status_code == 401:
            request = response.request
            response._handle_401 = getattr(response, '_handle_401', 0) + 1
            s_auth = response.headers.get('www-authenticate', '')
            if 'digest' in s_auth.lower() and response._handle_401 < 2:
                self.options = parse_dict_header(s_auth.replace('Digest ', ''))
                params = request.inp_params.copy()
                headers = params.pop('headers', [])
                headers.append(('authorization', self.encode(
                    request.method, request.url)))
                params['headers'] = headers
                response.request_again = request_again(request.method,
                                                       request.url,
                                                       params)

    def KD(self, s, d):
        return self.hex("%s:%s" % (s, d))

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
        raise ValueError
