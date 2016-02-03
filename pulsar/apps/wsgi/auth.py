'''
Handle basic and digest authentication on the server.

HttpAuthenticate
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: HttpAuthenticate
   :members:
   :member-order: bysource



Authorization header parser
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: parse_authorization_header
'''
import os
import time
from base64 import b64decode

from pulsar import HttpException
from pulsar.utils.httpurl import (parse_dict_header, hexmd5, hexsha1,
                                  quote_header_value, DEFAULT_CHARSET)
from pulsar.utils.pep import to_bytes


__all__ = ['HttpAuthenticate', 'parse_authorization_header']


_require_quoting = frozenset(['domain', 'nonce', 'opaque', 'realm'])


class HttpAuthenticate(HttpException):
    '''Exception when ``basic`` or ``digest`` authentication is required.

    This HttpException is raised with status code ``401`` and the extra
    ``WWW_Authenticate`` header if ``type`` is either ``basic`` or ``digest``.
    '''
    def __init__(self, type, realm=None, **options):
        realm = realm or 'authentication required'
        if type == 'basic':
            value = self._auth_header(type, realm=realm)
        elif type == 'digest':
            value = self.digest_auth_header(realm, **options)
        else:
            value = None
        if value:
            value = [('WWW-Authenticate', value)]
        super().__init__(status=401, headers=value)

    def digest_auth_header(self, realm=None, nonce=None, qop=None, opaque=None,
                           algorithm=None, stale=None):
        options = {}
        if nonce is None:
            nonce = hexmd5(to_bytes('%d' % time.time()) + os.urandom(10))
            if opaque is None:
                opaque = hexmd5(os.urandom(10))
        if stale:
            options['stale'] = 'TRUE'
        if opaque is not None:
            options['opaque'] = opaque
        if algorithm is not None:
            options['algorithm'] = algorithm
        if qop is None:
            qop = ('auth',)
        return self._auth_header('digest', realm=realm, nonce=nonce,
                                 qop=', '.join(qop), **options)

    def _auth_header(self, type, **options):
        """Convert the stored values into a WWW-Authenticate header."""
        return '%s %s' % (type.title(), ', '.join((
            '%s=%s' % (key, quote_header_value(
                value, allow_token=key not in _require_quoting))
            for key, value in options.items()
        )))


class BasicAuth:

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def authenticated(self, environ, username=None, password=None, **params):
        return username == self.username and password == self.password

    def __repr__(self):
        return 'Basic: %s' % self.username
    __str__ = __repr__


class DigestAuth:

    def __init__(self, username, password=None, options=None):
        self.username = username
        self.password = password
        self.last_nonce = None
        self.options = options or {}
        self.algorithm = self.options.pop('algorithm', 'MD5')

    def __repr__(self):
        return 'Digest: %s' % self.username
    __str__ = __repr__

    def authenticated(self, environ, username=None, password=None, **params):
        '''Called by the server to check if client is authenticated.'''
        if username != self.username:
            return False
        o = self.options
        qop = o.get('qop')
        method = environ['REQUEST_METHOD']
        uri = environ.get('PATH_INFO', '')
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


digest_parameters = frozenset(('username', 'realm', 'nonce', 'uri', 'nc',
                               'cnonce', 'response'))


def parse_authorization_header(value, charset='utf-8'):
    '''Parse an HTTP basic/digest authorisation header.

    :param value: the authorisation header to parse.
    :return: either `None` if the header was invalid or
        not given, otherwise an :class:`Auth` object.
    '''
    if not value:
        return
    try:
        auth_type, auth_info = value.split(None, 1)
        auth_type = auth_type.lower()
    except ValueError:
        return
    if auth_type == 'basic':
        try:
            up = b64decode(auth_info.encode(DEFAULT_CHARSET)).decode(charset)
            username, password = up.split(':', 1)
        except Exception:
            return
        return BasicAuth(username, password)
    elif auth_type == 'digest':
        auth_map = parse_dict_header(auth_info)
        if not digest_parameters.difference(auth_map):
            return DigestAuth(auth_map.pop('username'), options=auth_map)
