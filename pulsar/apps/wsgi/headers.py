import logging
from multidict import istr

from pulsar.utils.httpurl import tls_schemes


LOGGER = logging.getLogger('pulsar.wsgi')


COOKIE = istr('Cookie')
CONNECTION = istr('Connection')
CONTENT_LENGTH = istr('Content-Length')
CONTENT_TYPE = istr('Content-Type')
DATE = istr('Date')
HOST = istr('Host')
KEEP_ALIVE = istr('Keep-Alive')
LOCATION = istr('Location')
PROXY_AUTHENTICATE = istr('Proxy-Authenticate')
PROXY_AUTHORIZATION = istr('Proxy-Authorization')
SCRIPT_NAME = istr("Script_Name")
SERVER = istr('Server')
SET_COOKIE = istr('Set-Cookie')
TE = istr('Te')
TRAILERS = istr('Trailers')
TRANSFER_ENCODING = istr('Transfer-Encoding')
UPGRADE = istr('Upgrade')
X_FORWARDED_FOR = istr('X-Forwarded-For')
X_FORWARDED_PROTOCOL = istr("X-Forwarded-Protocol")
X_FORWARDED_PROTO = istr("X-Forwarded-Proto")
X_FORWARDED_SSL = istr("X-Forwarded-Ssl")

HOP_HEADERS = frozenset((
    CONNECTION, KEEP_ALIVE, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, TE, TRAILERS,
    TRANSFER_ENCODING, UPGRADE
))


def no_hop(headers):
    for header, value in headers:
        if header in HOP_HEADERS:
            # These features are the exclusive province of this class,
            # this should be considered a fatal error for an application
            # to attempt sending them, but we don't raise an error,
            # just log a warning
            LOGGER.warning('Application passing hop header "%s"', header)
            continue
        yield header, value


def x_forwarded_protocol(environ, value):
    if value == "ssl":
        environ['wsgi.url_scheme'] = 'https'


def x_forwarded_proto(environ, value):
    if value in tls_schemes:
        environ['wsgi.url_scheme'] = 'https'


def x_forwarded_ssl(environ, value):
    if value == "on":
        environ['wsgi.url_scheme'] = 'https'


def script_name(environ, value):
    environ['SCRIPT_NAME'] = value


def content_type(environ, value):
    environ['CONTENT_TYPE'] = value
    return True


def content_length(environ, value):
    environ['CONTENT_LENGTH'] = value
    return True


def host(environ, value):
    if not environ.get('wsgi.host'):
        environ['wsgi.host'] = value


HEADER_WSGI = {
    X_FORWARDED_PROTOCOL: x_forwarded_protocol,
    X_FORWARDED_PROTO: x_forwarded_proto,
    X_FORWARDED_SSL: x_forwarded_ssl,
    SCRIPT_NAME: script_name,
    CONTENT_TYPE: content_type,
    CONTENT_LENGTH: content_length
}
