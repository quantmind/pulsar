'''This module implements several WSGI middleware which does not
serve request but instead perform initialization and sanity checks.
It also introduces the **Response middlewares** implemented as a subclass of 
:class:`ResponseMiddleware`. Response middlewares are used by the
:class:`pulsar.apps.wsgi.wrappers.WsgiResponse` to modify/add headers and
manipulate content.

.. _wsgi-additional-middleware:

Additional WSGI Middlewares
============================

Several :ref:`wsgi middleware <wsgi-middleware>` useful in several applications.

clean path
~~~~~~~~~~~~~~~~~~
.. autofunction:: clean_path_middleware

cookie
~~~~~~~~~~~~~~~~~~
.. autofunction:: cookies_middleware

authorization
~~~~~~~~~~~~~~~~~~
.. autofunction:: authorization_middleware


.. _wsgi-response-middleware:

Response Middlewares
=============================

Response middleware are callable objects which can be used in conjunction
with pulsar :ref:`application handlers <wsgi-handlers>`. They must return
a :ref:`WsgiResponse <wsgi-response>` which can be the same as
the one passed to the callable or a brand new one.

Interface
~~~~~~~~~~~~~~~~~~

.. autoclass:: ResponseMiddleware
   :members:
   :member-order: bysource
   
GZip Middleware
~~~~~~~~~~~~~~~~~~~~~~
.. autoclass:: GZipMiddleware
   :members:
   :member-order: bysource
   
'''
import re
from gzip import GzipFile

import pulsar
from pulsar.utils.httpurl import BytesIO, parse_cookie

from .plugins import parse_authorization_header

re_accepts_gzip = re.compile(r'\bgzip\b')


__all__ = ['clean_path_middleware',
           'AccessControl',
           'GZipMiddleware',
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
    '''Clean url from double slashes and redirect if needed.'''
    path = environ['PATH_INFO']
    if path and '//' in path:
        url = re.sub("/+" , '/', path)
        if not url.startswith('/'):
            url = '/%s' % url
        qs = environ['QUERY_STRING']
        if qs:
            url = '%s?%s' % (url, qs)
        raise pulsar.HttpRedirect(url)

def cookies_middleware(environ, start_response):
    '''Parse the ``HTTP_COOKIE`` key in the ``environ`` and set
the new ``http.cookie`` key in ``environ`` with a dictionary of cookies
obtained via the :func:`pulsar.utils.httpurl.parse_cookie` function.'''
    c = environ.get('http.cookie')
    if not isinstance(c, dict):
        c = environ.get('HTTP_COOKIE', '')
        if not c:
            c = {}
        else:
            if not isinstance(c, str):
                c = c.encode('utf-8')
            c = parse_cookie(c)
        environ['http.cookie'] = c

def authorization_middleware(environ, start_response):
    """Parse the ``HTTP_AUTHORIZATION`` key in the ``environ`` if available
and set the ``http.authorization`` key in ``environ`` with the result
obtained from :func:`pulsar.apps.wsgi.plugins.parse_authorization_header`
function."""
    key = 'http.authorization'
    c = environ.get(key)
    if c is None:
        code = 'HTTP_AUTHORIZATION'
        if code in environ:
            environ[key] = parse_authorization_header(environ[code])

#####################################################    RESPONSE MIDDLEWARE
class ResponseMiddleware(object):
    '''Bas class for :class:`pulsar.apps.wsgi.wrappers.WsgiResponse`
middlewares. The focus of this class is the :meth:`execute` method where
the middleware logic is implemented.'''
    def version(self, environ):
        return environ.get('wsgi.version')

    def available(self, environ, response):
        '''Check if this :class:`ResponseMiddleware` can be applied to
the *response* object.

:param environ: a WSGI environ dictionary.
:param response: a :class:`pulsar.apps.wsgi.wrappers.WsgiResponse`
:return: ``True`` or ``False``.'''
        return True

    def __call__(self, environ, response):
        if not self.available(environ, response):
            return response
        resp = self.execute(environ, response)
        return resp if resp is not None else response

    def execute(self, environ, response):
        '''Manipulate *response*, called only if the :meth:`available`
method returns ``True``.'''
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
    """A :class:`ResponseMiddleware` for compressing content if the request
allows gzip compression. It sets the Vary header accordingly.

The compression implementation is from
http://jython.xhaus.com/http-compression-in-python-and-jython
    """
    def __init__(self, min_length=200):
        self.min_length = min_length

    def available(self, environ, response):
        # It's not worth compressing non-OK or really short responses
        try:
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
        except Exception:
            raise

    def execute(self, environ, response):
        headers = response.headers
        headers.add_header('Vary', 'Accept-Encoding')
        content = b''.join(response.content)
        response.content = (self.compress_string(content),)
        response.headers['Content-Encoding'] = 'gzip'

    def compress_string(self, s):
        zbuf = BytesIO()
        zfile = GzipFile(mode='wb', compresslevel=6, fileobj=zbuf)
        zfile.write(s)
        zfile.close()
        return zbuf.getvalue()
