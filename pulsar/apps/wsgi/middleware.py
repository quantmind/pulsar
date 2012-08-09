'''Response middleware
'''
import re
from gzip import GzipFile

import pulsar
from pulsar.utils.httpurl import BytesIO, parse_authorization_header,\
                                     parse_cookie

re_accepts_gzip = re.compile(r'\bgzip\b')


__all__ = ['AccessControl', 'GZipMiddleware',
           'cookies_middleware',
           'authorization_middleware',
           'is_streamed']


def is_streamed(content):
    try:
        len(content)
    except TypeError:
        return True
    return False


def clean_path_middleware(environ, start_response):
    '''Clean url and redirect if needed'''
    path = environ['PATH_INFO']
    if '//' in path:
        url = re.sub("/+" , '/', path)
        if not url.startswith('/'):
            url = '/%s' % url
        qs = environ['QUERY_STRING']
        if qs and environ['method'] == 'GET':
            url = '{0}?{1}'.format(url,qs)
        raise pulsar.HttpRedirect(url)

def cookies_middleware(environ, start_response):
    '''Parse the ``HTTP_COOKIE`` key in the *environ*. The ``HTTP_COOKIE``
string is replaced with a dictionary.'''
    c = environ.get('HTTP_COOKIE', '')
    if not isinstance(c, dict):
        if not c:
            c = {}
        else:
            if not isinstance(c, str):
                c = c.encode('utf-8')
            c = parse_cookie(c)
        environ['HTTP_COOKIE'] = c

def authorization_middleware(environ, start_response):
    """An `Authorization` middleware."""
    code = 'HTTP_AUTHORIZATION'
    if code in environ:
        environ[code] = parse_authorization_header(environ[code])

#####################################################    RESPONSE MIDDLEWARE
class ResponseMiddleware(object):

    def version(self, environ):
        return environ.get('wsgi.version')

    def available(self, environ, response):
        return True

    def __call__(self, environ, response):
        if not self.available(environ, response):
            return
        return self.execute(environ, response)

    def execute(self, environ, response):
        pass


class AccessControl(ResponseMiddleware):
    '''A response middleware which add the ``Access-Control-Allow-Origin``
response header.'''
    def __init__(self, origin='*', methods=None):
        self.origin = origin
        self.methods = methods

    def available(self, environ, response):
        return response.status_code == 200

    def execute(self, environ, response):
        response.headers['Access-Control-Allow-Origin'] = self.origin
        if self.methods:
            response.headers['Access-Control-Allow-Methods'] = self.methods


class GZipMiddleware(ResponseMiddleware):
    """Response middleware for compressing content if the browser allows
gzip compression. It sets the Vary header accordingly, so that caches will
base their storage on the Accept-Encoding header.
    """
    def __init__(self, min_length=200):
        self.min_length = min_length

    def available(self, environ, response):
        # It's not worth compressing non-OK or really short responses
        if response.status_code == 200 and not response.is_streamed:
            if response.length() < self.min_length:
                return False
            headers = response.headers
            # Avoid gzipping if we've already got a content-encoding.
            if 'Content-Encoding' in headers:
                return False
            # MSIE have issues with gzipped response of various content types.
            if "msie" in environ.get('HTTP_USER_AGENT', '').lower():
                ctype = headers.get('Content-Type', '').lower()
                if not ctype.startswith("text/") or "javascript" in ctype:
                    return False
            ae = environ.get('HTTP_ACCEPT_ENCODING', '')
            if not re_accepts_gzip.search(ae):
                return False
            return True

    def execute(self, environ, response):
        headers = response.headers
        headers.add_header('Vary', 'Accept-Encoding')
        content = b''.join(response.content)
        response.content = (self.compress_string(content),)
        response.headers['Content-Encoding'] = 'gzip'

    # From http://www.xhaus.com/alan/python/httpcomp.html#gzip
    def compress_string(self, s):
        zbuf = BytesIO()
        zfile = GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
        zfile.write(s)
        zfile.close()
        return zbuf.getvalue()


