import os

from multidict import istr


cdef bytes CRLF = b'\r\n'
cdef str CHARSET = 'ISO-8859-1'
cdef DEFAULT_HTTP = 'HTTP/1.1'

cdef str URL_SCHEME = os.environ.get('wsgi.url_scheme', 'http')
cdef str OS_SCRIPT_NAME = os.environ.get("SCRIPT_NAME", "")
cdef str PULSAR_CACHE = 'pulsar.cache'
cdef int MAX_CHUNK_SIZE = 65536
cdef object TLS_SCHEMES = frozenset(('https', 'wss'))
cdef object NO_CONTENT_CODES = frozenset((204, 304))
cdef object NO_BODY_VERBS = frozenset(('HEAD',))

cdef object COOKIE = istr('Cookie')
cdef object CONNECTION = istr('Connection')
cdef object CONTENT_LENGTH = istr('Content-Length')
cdef object CONTENT_TYPE = istr('Content-Type')
cdef object DATE = istr('Date')
cdef object EXPECT = istr('Expect')
cdef object HOST = istr('Host')
cdef object KEEP_ALIVE = istr('Keep-Alive')
cdef object LOCATION = istr('Location')
cdef object PROXY_AUTHENTICATE = istr('Proxy-Authenticate')
cdef object PROXY_AUTHORIZATION = istr('Proxy-Authorization')
cdef object SCRIPT_NAME = istr("Script_Name")
cdef object SERVER = istr('Server')
cdef object SET_COOKIE = istr('Set-Cookie')
cdef object TE = istr('Te')
cdef object TRAILERS = istr('Trailers')
cdef object TRANSFER_ENCODING = istr('Transfer-Encoding')
cdef object UPGRADE = istr('Upgrade')
cdef object X_FORWARDED_FOR = istr('X-Forwarded-For')
cdef object X_FORWARDED_PROTOCOL = istr("X-Forwarded-Protocol")
cdef object X_FORWARDED_PROTO = istr("X-Forwarded-Proto")
cdef object X_FORWARDED_SSL = istr("X-Forwarded-Ssl")

cdef object HOP_HEADERS = frozenset((
    CONNECTION, KEEP_ALIVE, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, TE, TRAILERS,
    TRANSFER_ENCODING, UPGRADE
))
cdef str _http_date_ = ''
cdef int _http_time_ = 0
